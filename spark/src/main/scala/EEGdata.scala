import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.rdd.RDDFunctions._

object EEGdata {
  def main(args: Array[String]) {

	val conf = new SparkConf().setAppName("EEG Data")
	val sc = new SparkContext(conf)

	val attention = sc.textFile("/data/csv/eeg/"+args(0)+".csv")
	val attention_data = attention.map(line => line.split(",")).map(col=>col(4))
	val attention_header = attention_data.first()
	val attention_values = attention_data.filter(row => row != attention_header)
	val attention_average = attention_values.map(_.toInt).sliding(20).map(attn => (attn.sum / attn.size))
	attention_average.saveAsTextFile("/data/csv/eeg/output/attention/"+args(0))

        val meditation = sc.textFile("/data/csv/eeg/"+args(0)+".csv")
        val meditation_data = meditation.map(line => line.split(",")).map(col=>col(5))
        val meditation_header = meditation_data.first()
        val meditation_values = meditation_data.filter(row => row !=  meditation_header)
        val meditation_average = meditation_values.map(_.toInt).sliding(20).map(medt => (medt.sum / medt.size))
        meditation_average.saveAsTextFile("/data/csv/eeg/output/meditation/"+args(0))

        val blink = sc.textFile("/data/csv/eeg/"+args(0)+".csv")
        val blink_data = blink.map(line => line.split(",")).map(col=>col(4))
        val blink_header = blink_data.first()
        val blink_values = blink_data.filter(row => row != blink_header)
	val blink_count = blink_values.map(_.toInt).sliding(2).map(b => Math.abs(b(0)-b(1))/20)
        blink_count.saveAsTextFile("/data/csv/eeg/output/blink/"+args(0))
 }
}
