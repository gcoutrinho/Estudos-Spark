import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class DesafioSpark {
 
	
	public static void main(String args[]) {
		
		 SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkMinarator");
         JavaSparkContext ctx = new JavaSparkContext(conf);
         
         //Carrega o conteúdo do TXT para dentro do RDD do Spark
         JavaRDD<String> rddAugust = ctx.textFile("C:/Users/William/eclipse-workspace/AnaliseDeDados/access_log_Aug95");
         JavaRDD<String> rddJuly = ctx.textFile("C:/Users/William/eclipse-workspace/AnaliseDeDados/access_log_Jul95");
         
         //Une os dois RDDs dentro de um unico
         JavaRDD<String> unionRDD = rddJuly.union(rddAugust);
         
         //Imprime os numeros de Host retornado no console.
         System.out.println("Numero de Hosta unicos: " + retornaNumeroDeHostsUnicos(unionRDD));
         
         //Imprime a quantidade de bytes retornados
         System.out.println("Total de Bytes: " + retornaTotalDeBytesRtornados(unionRDD));
         
         //Retorna top 5 urls que retornaram mais erros
         retornaTop5UrlErros(unionRDD);  
         
         //Retorna o total de erros 404 encontrados.
        System.out.println("Total e erros 404 encontrados: " + retornaNumeroDeErros404Econtrados(unionRDD));
        
        //Retorna a quantidade de erros 404 por dia
        retornaErros404PorDia(unionRDD);
	}
	
	public static void retornaErros404PorDia(JavaRDD<String> unionRDD) {
		JavaRDD<String> errosFiltrados = unionRDD.filter(x -> x.contains("404"));
		JavaPairRDD<String, Integer> filtered = errosFiltrados
				.mapToPair(m -> new Tuple2<String, Integer>(m.split("-.-\\w|\\[|\\]|\"|")[4].split(":")[4], 1));
		
		JavaPairRDD<String, Integer> agrupaErrosPorDia = filtered.reduceByKey((x, y) -> x + y);	
		
		agrupaErrosPorDia.foreach((f) -> System.out.println(f));
	}
	
	public static Long retornaNumeroDeErros404Econtrados(JavaRDD<String> unionRDD) {
		
		JavaRDD<String> totalErrosEncontrados = unionRDD.filter(x -> x.contains("404"));
		
		return totalErrosEncontrados.count();	
		
	}

	private static void retornaTop5UrlErros(JavaRDD<String> unionRDD) {
		JavaRDD<String> filtraErros404 = unionRDD.filter(x -> x.contains("404"));
		
		JavaPairRDD<String, Integer> pairErros = filtraErros404.
											mapToPair(x -> new Tuple2<String, Integer>(x.split("-.-\\w|\\[|\\]|\"|")[0], 1));
		
		//Agrupa e ordena os hosts top 5 erros
		List<Tuple2<String, Integer>> sorteErros404 = pairErros.reduceByKey((x, y) -> x + y).sortByKey(true).take(5);
		
		for (Tuple2<String, Integer> hostName : sorteErros404) {
			System.out.println(hostName._1);
		}	
	
	}

	private static Integer retornaTotalDeBytesRtornados(JavaRDD<String> unionRDD) {
		
		//Usa o flatMap para semparar as strings
		JavaRDD<Integer> dataFiltered = unionRDD.flatMap(t -> {
			try {
				return Arrays.asList(Integer.parseInt(t.split("-.-\\w|\\[|\\]|\"|")[13])).iterator();

			} catch (Exception e) {
				return Arrays.asList(0).iterator();
			}
		});

		//Soma total de byte
		return dataFiltered.reduce((a, b) -> a + b);
		
	}

	private static Integer retornaNumeroDeHostsUnicos(JavaRDD<String> unionRDD) {	
	
		 JavaPairRDD<String, Integer> host = unionRDD
				    .mapToPair(s -> new Tuple2<String, Integer>(s.split("-.-\\w|\\[|\\]|\"|")[0], 1));
				  JavaPairRDD<String, Integer> hostsAgrupados = host.reduceByKey((x, y) -> x + y);
				  
				  Integer totalHostsUnicos = (int) hostsAgrupados.filter(x -> x._2 == 1).count();
				
		return totalHostsUnicos;
	}
	
	
}