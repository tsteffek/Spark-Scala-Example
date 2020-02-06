import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object WordCount extends App {
  val name = "Word Count"
  val spark = SparkSession.builder
    .master("local")
    .config("spark.app.id", name)
    .appName(name)
    .getOrCreate()

  val shakespeareTxt = "./src/main/resources/t8.shakespeare.txt"
  val shakespeare: RDD[String] = spark.read
    .option("wholetext", true)
    .textFile(shakespeareTxt)
    .persist(StorageLevel.MEMORY_ONLY)
    .rdd

  val works: RDD[String] = shakespeare
    .flatMap(wholeText => wholeText.split("<<") // start of copyright disclaimer
      .drop(2) // drop the first 2, they are headers
      .dropRight(1) // drop the last 1, it tells you about the end
    )
    .map(work => work.split(">>")(1)) // cut out all copyright disclaimers

  val sortedWordCounts: RDD[(String, Int)] = works
    //    .flatMap(line => line.split(" +"))
    //    .flatMap(line => line.split("\\s+"))
    //    .flatMap(line => line.split("[^\\w']+"))
    .flatMap(line => line.split("\\W+"))
    .map(word => (word.toLowerCase, 1))
    .reduceByKey(_ + _)

  sortedWordCounts
    .top(30)(Ordering.by(_._2))
    .zipWithIndex
    .foreach { case (wordWithCount, index) => println(s"${index + 1}: $wordWithCount") }

  spark.stop()
}
