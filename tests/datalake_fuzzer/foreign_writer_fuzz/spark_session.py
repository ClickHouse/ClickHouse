"""One Spark session that can write every format the harness uses.

Iceberg (Hadoop catalog on `s3a`), Delta (path tables on `s3a`), Hudi (path tables on `s3a`) and
Paimon (local warehouse, uploaded after each commit) share the session, so a test module pays the
JVM start-up once. The jars come from `$SPARK_HOME/jars` of the integration runner image.
"""

import logging

from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.spark_tools import ResilientSparkSession, write_spark_log_config

from .writers import spark_iceberg, spark_paimon


def build_spark(cluster, app_name="foreign_writer_fuzz"):
    logging.getLogger("py4j").setLevel(logging.ERROR)
    import pyspark

    config = {
        "spark.sql.extensions": ",".join(
            [
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "io.delta.sql.DeltaSparkSessionExtension",
                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
            ]
        ),
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.session.timeZone": "UTC",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        "spark.databricks.delta.writeChecksumFile.enabled": "false",
        "spark.sql.shuffle.partitions": "2",
        "spark.default.parallelism": "2",
        "spark.ui.enabled": "false",
        "spark.driver.memory": "4g",
        "spark.hadoop.fs.s3a.endpoint": f"http://{cluster.minio_ip}:{cluster.minio_port}/",
        "spark.hadoop.fs.s3a.access.key": minio_access_key,
        "spark.hadoop.fs.s3a.secret.key": minio_secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }
    config.update(spark_iceberg.spark_catalog_config(cluster.minio_bucket))
    config.update(spark_paimon.spark_catalog_config())

    def create():
        builder = pyspark.sql.SparkSession.builder.appName(app_name).master("local[2]")
        for key, value in config.items():
            builder = builder.config(key, value)
        props_path = write_spark_log_config(cluster.instances_dir)
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"-Dlog4j2.configurationFile=file:{props_path}",
        )
        return builder.getOrCreate()

    return ResilientSparkSession(create)
