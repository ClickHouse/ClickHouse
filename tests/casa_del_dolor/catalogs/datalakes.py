import logging
import os
import random
import shutil
import socket
import subprocess
import time
import threading

from pathlib import Path
from integration.helpers.client import Client
from pyiceberg.catalog import load_catalog
from .kafkatest import KafkaHandler
from .laketables import (
    TableStorage,
    LakeFormat,
    LakeCatalogs,
    SparkTable,
)
from .tablegenerator import LakeTableGenerator, sample_from_dict, true_false_lambda
from .datagenerator import LakeDataGenerator
from .tablecheck import SparkAndClickHouseCheck

from utils.backgroundworker import BackgroundWorker


"""
┌─────────────────┬────────────────┬──────────────────────────────────────┐
│ Catalog Type    │ Mixed Support  │ How It Works                         │
├─────────────────┼────────────────┼──────────────────────────────────────┤
│ Hive Metastore  │ ✅ YES         │ Native support for multiple formats  │
│ AWS Glue        │ ✅ YES         │ Stores format in table metadata      │
│ Unity Catalog   │ ✅ YES         │ Designed for multi-format support    │
│ Hadoop (Iceberg)│ ❌ NO          │ Iceberg-specific catalog             │
│ Nessie          │ ⚠️  PARTIAL    │ Primarily Iceberg, Delta via Hive    │
│ REST (Iceberg)  │ ❌ NO          │ Iceberg-specific catalog             │
│ Delta Catalog   │ ❌ NO          │ Delta-specific catalog               │
└─────────────────┴────────────────┴──────────────────────────────────────┘
┌─────────────────┬────────────────┬─────────────────────────────────────────┐
│ Catalog Type    │ Can Create?    │ What You Can Actually Do                │
├─────────────────┼────────────────┼─────────────────────────────────────────┤
│ Hadoop          │ ✅ YES         │ Create by configuring in SparkSession   │
│ Hive Metastore  │ ⚠️  PARTIAL    │ Can initialize schema, not the service  │
│ AWS Glue        │ ❌ NO          │ Must exist in AWS first                 │
│ REST            │ ❌ NO          │ Requires running REST server            │
│ Nessie          │ ❌ NO          │ Requires running Nessie server          │
│ Unity (DBR)     │ ✅ YES*        │ Can create via SQL in Databricks        │
│ In-Memory       │ ✅ YES         │ Create temporary catalog in session     │
└─────────────────┴────────────────┴─────────────────────────────────────────┘
"""


def get_local_base_path(catalog_name: str) -> str:
    return f"/var/lib/clickhouse/user_files/lakehouses/{catalog_name}"


spark_properties = {
    "spark.databricks.delta.checkLatestSchemaOnRead": true_false_lambda,
    "spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled": true_false_lambda,
    "spark.databricks.delta.metricsCollection.enabled": true_false_lambda,
    "spark.databricks.delta.optimize.maxFileSize": lambda: random.choice(
        ["134217728", "268435456", "1073741824"]
    ),
    # "spark.databricks.delta.optimize.minFileSize" : lambda: random.choice(["10485760", "20971520", "104857600"]),
    "spark.databricks.delta.optimize.repartition.enabled": true_false_lambda,
    "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": true_false_lambda,
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": true_false_lambda,
    "spark.databricks.delta.schema.autoMerge.enabled": true_false_lambda,
    "spark.databricks.delta.stalenessLimit": lambda: random.choice(
        ["1h", "6h", "12h", "24h"]
    ),
    "spark.databricks.delta.vacuum.parallelDelete.enabled": true_false_lambda,
    "spark.sql.adaptive.coalescePartitions.enabled": true_false_lambda,
    "spark.sql.adaptive.localShuffleReader.enabled": true_false_lambda,
    "spark.sql.adaptive.skewJoin.enabled": true_false_lambda,
    "spark.sql.ansi.enabled": true_false_lambda,
    "spark.sql.autoBroadcastJoinThreshold": lambda: random.choice(
        ["-1", "1MB", "10MB", "48MB"]
    ),
    "spark.sql.caseSensitive": true_false_lambda,
    "spark.sql.codegen.fallback": true_false_lambda,
    "spark.sql.codegen.wholeStage": true_false_lambda,
    "spark.sql.execution.sortBeforeRepartition": true_false_lambda,
    "spark.sql.iceberg.check-committer-thread": true_false_lambda,
    "spark.sql.iceberg.compress-metadata": true_false_lambda,
    "spark.sql.iceberg.handle-timestamp-without-timezone": true_false_lambda,
    "spark.sql.iceberg.merge-on-read.enabled": true_false_lambda,
    "spark.sql.iceberg.planning.locality.enabled": true_false_lambda,
    "spark.sql.iceberg.planning.max-table-cache-size": lambda: random.choice(
        ["10", "50", "100"]
    ),
    "spark.sql.iceberg.use-native-partition-data": true_false_lambda,
    "spark.sql.iceberg.vectorization.enabled": true_false_lambda,
    "spark.sql.iceberg.write.distribution.mode": lambda: random.choice(
        ["none", "hash", "range"]
    ),
    "spark.sql.iceberg.write.fanout.enabled": true_false_lambda,
    "spark.sql.inMemoryColumnarStorage.batchSize": lambda: random.choice(
        ["10", "100", "1000"]
    ),
    "spark.sql.inMemoryColumnarStorage.compressed": true_false_lambda,
    "spark.sql.join.preferSortMergeJoin": true_false_lambda,
    "spark.sql.jsonGenerator.ignoreNullFields": true_false_lambda,
    "spark.sql.orc.compression.codec": lambda: random.choice(
        ["snappy", "zlib", "zstd", "uncompressed"]
    ),
    "spark.sql.orc.enableVectorizedReader": true_false_lambda,
    "spark.sql.orc.filterPushdown": true_false_lambda,
    "spark.sql.orc.impl": lambda: random.choice(["native", "hive"]),
    "spark.sql.parquet.compression.codec": lambda: random.choice(
        ["uncompressed", "snappy", "gzip", "zstd"]
    ),
    "spark.sql.parquet.enableDictionary": true_false_lambda,
    "spark.sql.parquet.enableVectorizedReader": true_false_lambda,
    "spark.sql.parquet.filterPushdown": true_false_lambda,
    "spark.sql.parquet.mergeSchema": true_false_lambda,
    "spark.sql.parquet.outputTimestampType": lambda: random.choice(
        ["INT96", "TIMESTAMP_MICROS", "TIMESTAMP_MILLIS"]
    ),
    "spark.sql.parquet.writeLegacyFormat": true_false_lambda,
    # "spark.sql.shuffle.partitions": lambda: random.choice( don't set this too low
    #    ["8", "16", "32", "64", "200", "400"]
    # ),
    "spark.sql.sources.partitionOverwriteMode": lambda: random.choice(
        ["static", "dynamic"]
    ),
    "spark.sql.statistics.autogather": true_false_lambda,
}


def wait_for_port(host, port, timeout=90):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with socket.create_connection((host, port), 1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


class DolorCatalog:

    def __init__(
        self,
        _catalog_name: str,
        _catalog_type: LakeCatalogs,
        _catalog_impl,
    ):
        self.catalog_name = _catalog_name
        self.catalog_type = _catalog_type
        self.catalog_impl = _catalog_impl
        self.spark_tables: dict[str, SparkTable] = {}


class SparkHandler:
    def __init__(self, cluster, with_unity: bool, env: dict[str, str]):
        self.logger = logging.getLogger(__name__)
        self.catalogs_lock = threading.Lock()
        self.spark_lock = threading.Lock()
        self.uc_server = None
        self.catalogs = {}
        self.uc_server_dir = None
        if with_unity is not None:
            self.uc_server_dir = with_unity
            self.uc_server_run_dir = Path(cluster.instances_dir) / "ucserver"

        self.spark_log_dir = Path(cluster.instances_dir) / "spark"
        self.spark_log_config = Path(cluster.instances_dir) / "log4j2.properties"
        self.spark_query_logger = self.spark_log_dir / "query.log"
        self.derby_logger = self.spark_log_dir / "derby.log"
        self.metastore_db = self.spark_log_dir / "metastore_db"
        self.data_generator = LakeDataGenerator(self.spark_query_logger)
        self.table_check = SparkAndClickHouseCheck()
        self.env = env
        spark_log = f"""
# ----- log4j2.properties -----
status = error
name = SparkLog

# Where to store logs (change as needed; use an absolute path on clusters)
property.log.dir = {self.spark_log_dir}
property.app.name = ${{sys:spark.app.name:-spark-app}}

appender.console.type = Console
appender.console.name = console
appender.console.target = SYSTEM_ERR
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{{yy/MM/dd HH:mm:ss}} %p %c{{1}}: %m%n

appender.file.type = RollingFile
appender.file.name = file
appender.file.fileName = ${{log.dir}}/${{app.name}}-driver.log
appender.file.filePattern = ${{log.dir}}/${{app.name}}-driver-%d{{yyyy-MM-dd}}-%i.log.gz
appender.file.layout.type = PatternLayout
appender.file.layout.pattern = %d{{ISO8601}} %-5p %c{{1}}:%L - %m%n
appender.file.policies.type = Policies
appender.file.policies.time.type = TimeBasedTriggeringPolicy
appender.file.policies.time.interval = 1
appender.file.policies.time.modulate = true
appender.file.policies.size.type = SizeBasedTriggeringPolicy
appender.file.policies.size.size = 100MB
appender.file.strategy.type = DefaultRolloverStrategy
appender.file.strategy.max = 30

rootLogger.level = info
rootLogger.appenderRefs = console, file
rootLogger.appenderRef.file.ref = file

# Quiet some noisy libs if you like:
logger.jetty.name = org.eclipse.jetty
logger.jetty.level = warn
"""
        with open(self.spark_log_config, "w+") as f:
            f.write(spark_log)

        # Setup background worker for later
        def my_task():
            pass

        self.worker = BackgroundWorker(my_task, interval=1)
        self.worker.start()
        if cluster.with_kafka:
            self.kafka_handler = KafkaHandler(cluster)

    def start_uc_server(self):
        with self.catalogs_lock:
            if self.uc_server is None:
                if self.uc_server_dir is None:
                    raise RuntimeError(
                        "with_unity argument with path to unity server dir is required"
                    )
                java = shutil.which("java")
                if not java:
                    raise RuntimeError(
                        "Java not found on PATH. Install JDK 17 and/or set JAVA_HOME."
                    )

                uc_port = 8085
                uc_server_bin = self.uc_server_dir / "bin" / "start-uc-server"
                cmd = [str(uc_server_bin), "-p", str(uc_port)]
                os.makedirs(self.uc_server_run_dir, exist_ok=True)
                # Start the server
                with open(self.uc_server_run_dir / "uc_server_bin.log", "w+") as f1:
                    with open(
                        self.uc_server_run_dir / "uc_server_bin.err.log", "w+"
                    ) as f2:
                        self.uc_server = subprocess.Popen(
                            cmd,
                            cwd=str(self.uc_server_run_dir),
                            env=os.environ.copy(),
                            stdout=f1,
                            stderr=f2,
                            text=True,
                            bufsize=1,
                        )
                self.logger.info(f"Starting UC server using pid = {self.uc_server.pid}")
                if not wait_for_port("localhost", uc_port, timeout=120):
                    raise TimeoutError(
                        f"UC server did not start on localhost:{uc_port} within timeout"
                    )
                self.logger.info(
                    f"UC server is accepting connections on http://localhost:{uc_port}"
                )

    def run_unity_cmd(self, args: list[str]):
        if self.uc_server_dir is None:
            raise RuntimeError(
                "with_unity argument with path to unity server dir is required"
            )
        cmd: list[str] = [str(self.uc_server_dir / "bin" / "uc")]
        cmd.extend(args)
        result = subprocess.run(
            cmd,
            cwd=str(self.uc_server_run_dir),
            env=os.environ.copy(),
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            self.logger.error(
                f"UC CLI failed (exit {result.returncode}).\n"
                f"Command: {' '.join(cmd)}\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )

    def get_spark(
        self,
        cluster,
        env,
        sparklogfile: str,
        derbylogfile: str,
        metastore_db: str,
        catalog_name: str,
        storage: TableStorage,
        lake: LakeFormat,
        catalog: LakeCatalogs,
    ):
        selected_properties = {}
        # ============================================================
        # CORE CONFIGURATIONS FOR BOTH ICEBERG AND DELTA
        # ============================================================
        catalog_extension = ""
        catalog_format = ""
        all_jars = [
            "io.delta:delta-spark_2.13:4.0.0",
            "io.unitycatalog:unitycatalog-spark_2.13:0.3.0",
            "org.apache.iceberg:iceberg-aws-bundle:1.10.0",
            "org.apache.iceberg:iceberg-azure-bundle:1.10.0",
            "org.apache.iceberg:iceberg-spark-extensions-4.0_2.13:1.10.0",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0",
            "org.apache.spark:spark-hadoop-cloud_2.13:4.0.1",
            # Derby jars
            "org.apache.derby:derby:10.14.2.0",
            "org.apache.derby:derbytools:10.14.2.0",
            "org.datanucleus:datanucleus-core:4.1.17",
            "org.datanucleus:datanucleus-api-jdo:4.2.4",
            "org.datanucleus:datanucleus-rdbms:4.1.19",
        ]
        if lake == LakeFormat.Iceberg:
            catalog_extension = (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            )
            catalog_format = "org.apache.iceberg.spark.SparkCatalog"
        elif lake == LakeFormat.DeltaLake:
            catalog_extension = "io.delta.sql.DeltaSparkSessionExtension"
            catalog_format = (
                "io.unitycatalog.spark.UCSingleCatalog"
                if catalog == LakeCatalogs.Unity
                else "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
        else:
            raise Exception("Unknown lake format")

        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--packages {','.join(all_jars)} pyspark-shell"
        )
        for k, val in env.items():
            os.environ[k] = val

        if random.randint(1, 100) <= 70:
            selected_properties = sample_from_dict(
                spark_properties, random.randint(0, len(spark_properties))
            )

        with self.spark_lock:
            from pyspark.sql import SparkSession

            builder = SparkSession.builder
            builder.config("spark.sql.extensions", catalog_extension)
            builder.config(f"spark.sql.catalog.{catalog_name}", catalog_format)
            builder.config(
                "spark.driver.extraJavaOptions",
                f"-Dlog4j.configurationFile=file:{sparklogfile} -Dderby.stream.error.file={derbylogfile}",
            )
            builder.config(
                "spark.executor.extraJavaOptions",
                f"-Dlog4j.configurationFile=file:{sparklogfile} -Dderby.stream.error.file={derbylogfile}",
            )
            builder.config("spark.driver.memory", "16g")
            builder.config("spark.driver.memoryOverhead", "2g")

            # ============================================================
            # CATALOG CONFIGURATIONS
            # ============================================================
            if catalog == LakeCatalogs.Glue:
                # Mock AWS credentials
                builder.config("spark.driverEnv.AWS_ACCESS_KEY_ID", "testing")
                builder.config("spark.driverEnv.AWS_SECRET_ACCESS_KEY", "testing")
                builder.config("spark.driverEnv.AWS_SESSION_TOKEN", "testing")
                builder.config("spark.executorEnv.AWS_ACCESS_KEY_ID", "testing")
                builder.config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", "testing")
                builder.config("spark.executorEnv.AWS_SESSION_TOKEN", "testing")

                if lake == LakeFormat.Iceberg:
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.catalog-impl",
                        "org.apache.iceberg.aws.glue.GlueCatalog",
                    )
                elif lake == LakeFormat.DeltaLake:
                    builder.config(
                        "spark.hadoop.hive.metastore.client.factory.class",
                        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                    )

                if storage == TableStorage.S3:
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.io-impl",
                        "org.apache.iceberg.aws.s3.S3FileIO",
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.endpoint",
                        f"http://{cluster.get_instance_ip('minio')}:{cluster.minio_s3_port}",
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.access-key-id",
                        cluster.minio_access_key,
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.secret-access-key",
                        cluster.minio_secret_key,
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.signing-region",
                        "us-east-1",
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true"
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.glue.endpoint",
                        f"http://localhost:{cluster.glue_catalog_port}",
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.glue.region", "us-east-1"
                    )

                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.warehouse",
                        "s3://warehouse-glue/data",
                    )
            elif catalog == LakeCatalogs.Hive:
                builder.config(
                    "spark.sql.catalog.hive.catalog-impl",
                    "org.apache.iceberg.hive.HiveCatalog",
                )

                builder.config(
                    f"spark.sql.catalog.{catalog_name}.uri",
                    f"thrift://0.0.0.0:{cluster.hms_catalog_port}",
                )
                if storage == TableStorage.S3:
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.io-impl",
                        "org.apache.iceberg.aws.s3.S3FileIO",
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.endpoint",
                        f"http://{cluster.get_instance_ip('minio')}:{cluster.minio_s3_port}",
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.access-key-id",
                        cluster.minio_access_key,
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.secret-access-key",
                        cluster.minio_secret_key,
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.signing-region",
                        "us-east-1",
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.path-style-access",
                        "true",
                    )

                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.warehouse",
                        "s3a://warehouse-hms/data",
                    )
            elif catalog == LakeCatalogs.REST or (
                catalog == LakeCatalogs.Unity and lake == LakeFormat.Iceberg
            ):
                builder.config(
                    f"spark.sql.catalog.{catalog_name}.catalog-impl",
                    "org.apache.iceberg.rest.RESTCatalog",
                )
                builder.config(
                    f"spark.sql.catalog.{catalog_name}.uri",
                    f"http://localhost:{'8085/api/2.1/unity-catalog/iceberg' if catalog == LakeCatalogs.Unity else cluster.iceberg_rest_catalog_port}",
                )
                if storage == TableStorage.S3:
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.io-impl",
                        "org.apache.iceberg.aws.s3.S3FileIO",
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.endpoint",
                        f"http://{cluster.get_instance_ip('minio')}:{cluster.minio_s3_port}",
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.access-key-id",
                        cluster.minio_access_key,
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.secret-access-key",
                        cluster.minio_secret_key,
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.signing-region",
                        "us-east-1",
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true"
                    )

                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.warehouse",
                        "s3://warehouse-rest/data",
                    )
            elif catalog == LakeCatalogs.Unity and lake == LakeFormat.DeltaLake:
                builder.config(
                    f"spark.sql.catalog.{catalog_name}.uri", "http://localhost:8085"
                )
                builder.config(f"spark.sql.catalog.{catalog_name}.token", "")
                builder.config("spark.sql.defaultCatalog", f"{catalog_name}")
            elif lake == LakeFormat.Iceberg:
                # Something as default for iceberg
                builder.config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
            elif lake == LakeFormat.DeltaLake:
                # Enable persistence
                builder.config(
                    "spark.hadoop.javax.jdo.option.ConnectionURL",
                    f"jdbc:derby:;databaseName={metastore_db};create=true",
                )
                builder.config(
                    "javax.jdo.option.ConnectionDriverName",
                    "org.apache.derby.jdbc.EmbeddedDriver",
                )
                builder.config("datanucleus.schema.autoCreateAll", "true")
                builder.config("datanucleus.fixedDatastore", "false")
                builder.config("spark.sql.catalogImplementation", "hive")
                builder.enableHiveSupport()

            # ============================================================
            # STORAGE CONFIGURATIONS
            # ============================================================
            if storage == TableStorage.S3:
                # Driver environment
                builder.config(
                    "spark.driverEnv.MINIO_ACCESS_KEY", cluster.minio_access_key
                )
                builder.config(
                    "spark.driverEnv.MINIO_SECRET_KEY", cluster.minio_secret_key
                )
                # Executor environment
                builder.config(
                    "spark.executorEnv.MINIO_ACCESS_KEY", cluster.minio_access_key
                )
                builder.config(
                    "spark.executorEnv.MINIO_SECRET_KEY", cluster.minio_secret_key
                )

                if catalog not in (LakeCatalogs.Glue, LakeCatalogs.REST):
                    # S3A filesystem implementation
                    builder.config(
                        "spark.hadoop.fs.s3a.impl",
                        "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    )
                    # MinIO endpoint and credentials
                    builder.config(
                        "spark.hadoop.fs.s3a.endpoint",
                        f"http://{cluster.minio_ip}:{cluster.minio_port}",
                    )
                    builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
                    builder.config(
                        "spark.hadoop.fs.s3a.connection.ssl.enabled", "false"
                    )
                    builder.config(
                        "spark.hadoop.fs.s3a.access.key", cluster.minio_access_key
                    )
                    builder.config(
                        "spark.hadoop.fs.s3a.secret.key", cluster.minio_secret_key
                    )
                    builder.config(
                        "spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                    )

                if catalog == LakeCatalogs.NoCatalog:
                    builder.config(
                        "spark.sql.warehouse.dir",
                        f"s3a://{cluster.minio_bucket}/{catalog_name}",
                    )
                    builder.config(
                        f"spark.sql.catalog.{catalog_name}.warehouse",
                        f"s3a://{cluster.minio_bucket}/{catalog_name}",
                    )
            elif storage == TableStorage.Azure:
                # For Azurite local emulation
                builder.config(
                    f"spark.hadoop.fs.azure.storage.emulator.account.name",
                    cluster.azurite_account,
                )
                builder.config(
                    f"spark.hadoop.fs.azure.account.key.{cluster.azurite_account}.blob.core.windows.net",
                    cluster.azurite_key,
                )
                # WASB implementation, ABFS is not compatible with Azurite?
                builder.config(
                    "spark.hadoop.fs.wasb.impl",
                    "org.apache.hadoop.fs.azure.NativeAzureFileSystem",
                )
                builder.config("spark.hadoop.fs.azure.always.use.https", "false")
                builder.config("spark.hadoop.fs.azure.ssl.enabled", "false")

                builder.config(
                    "spark.sql.warehouse.dir",
                    f"wasb://{cluster.azure_container_name}@{cluster.azurite_account}.blob.core.windows.net/{catalog_name}",
                )
                builder.config(
                    f"spark.sql.catalog.{catalog_name}.warehouse",
                    f"wasb://{cluster.azure_container_name}@{cluster.azurite_account}.blob.core.windows.net/{catalog_name}",
                )
            elif storage == TableStorage.Local:
                os.makedirs(get_local_base_path(catalog_name), exist_ok=True)

                builder.config(
                    "spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem"
                )
                builder.config(
                    "spark.sql.warehouse.dir",
                    f"file://{get_local_base_path(catalog_name)}",
                )
                builder.config(
                    f"spark.sql.catalog.{catalog_name}.warehouse",
                    f"file://{get_local_base_path(catalog_name)}",
                )
            else:
                raise Exception("Unknown storage")

            # ============================================================
            # FILE FORMAT CONFIGURATIONS
            # ============================================================
            # builder.config("spark.sql.sources.default", "parquet")
            # builder.config(f"spark.sql.catalog.{catalog_name}.write.format.default", "parquet")
            # builder.config(
            #    f"spark.sql.catalog.{catalog_name}.write.parquet.compression-codec", "snappy"
            # )
            # builder.config(
            #    f"spark.sql.catalog.{catalog_name}.compression.enabled",
            #    random.choice(["true", "false"]),
            # )
            # Support very old dates
            builder.config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
            builder.config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
            # Allow multiple sessions with different settings
            builder.config("spark.sql.adaptive.enabled", "true")
            # Allow to retain 0 hours on vacuum
            builder.config(
                "spark.databricks.delta.retentionDurationCheck.enabled", "false"
            )
            builder.config("spark.hadoop.zlib.compress.level", "DEFAULT_COMPRESSION")
            # Set timezone to match ClickHouse
            if "TZ" in env:
                builder.config("spark.sql.session.timeZone", env["TZ"])
            for k, val in env.items():
                builder.config(f"spark.executorEnv.{k}", val)

            # Random properties
            for key, val in selected_properties.items():
                builder.config(key, val())

            return builder.getOrCreate()
        return None

    def get_next_session(
        self,
        cluster,
        catalog_name: str,
        next_storage: TableStorage,
        next_lake: LakeFormat,
        next_catalog: LakeCatalogs,
    ):
        return self.get_spark(
            cluster,
            self.env,
            str(self.spark_log_config),
            str(self.derby_logger),
            str(self.metastore_db),
            catalog_name,
            next_storage,
            next_lake,
            next_catalog,
        )

    def run_query(self, session, query: str):
        self.logger.info(f"Running query: {query}")
        # Ignore spark_query_logger at the moment because this is multithreaded
        # with open(self.spark_query_logger, "a") as f:
        #    f.write(query + "\n")
        session.sql(query)

    def create_database(self, session, catalog_name: str):
        next_sql = f"CREATE DATABASE IF NOT EXISTS {catalog_name}.test;"
        self.run_query(session, next_sql)

    def create_lake_database(self, cluster, data) -> bool:
        saved_exception = None
        catalog = data["catalog"]
        catalog_name = data["database_name"]
        next_storage = TableStorage.storage_from_str(data["storage"])
        next_lake = LakeFormat.lakeformat_from_str(data["engine"])
        next_catalog = LakeCatalogs.catalog_from_str(catalog)
        next_catalog_impl = None

        # Load catalog if needed
        if next_lake == LakeFormat.Iceberg and next_storage == TableStorage.S3:
            params = {
                "s3.endpoint": f"http://{cluster.get_instance_ip('minio')}:{cluster.minio_s3_port}",
                "s3.access-key-id": cluster.minio_access_key,
                "s3.secret-access-key": cluster.minio_secret_key,
                "s3.signing-region": "us-east-1",
                "s3.path-style-access": "true",
                "s3.connection-ssl.enabled": "false",
            }
            if next_catalog == LakeCatalogs.REST:
                params.update(
                    {
                        "type": "rest",
                        "uri": f"http://localhost:{cluster.iceberg_rest_catalog_port}",
                    }
                )
            elif next_catalog == LakeCatalogs.Glue:
                params.update(
                    {
                        "type": "glue",
                        "glue.endpoint": f"http://localhost:{cluster.glue_catalog_port}",
                        "glue.region": "us-east-1",
                        "glue.access-key-id": cluster.minio_access_key,
                        "glue.secret-access-key": cluster.minio_secret_key,
                    }
                )
            elif next_catalog == LakeCatalogs.Hive:
                params.update(
                    {
                        "type": "hive",
                        "uri": f"thrift://0.0.0.0:{cluster.hms_catalog_port}",
                        "client.region": "us-east-1",
                    }
                )
            else:
                raise Exception("I have not implemented this case yet")
            next_catalog_impl = load_catalog(catalog_name, **params)
        elif next_catalog == LakeCatalogs.Unity:
            self.start_uc_server()
            self.logger.info(f"Creating unity catalog {catalog_name}")
            self.run_unity_cmd(["catalog", "create", "--name", catalog_name])
        else:
            raise Exception("I have not implemented this case yet")

        with self.catalogs_lock:
            self.catalogs[catalog_name] = DolorCatalog(
                catalog_name,
                next_catalog,
                next_catalog_impl,
            )
        if next_lake == LakeFormat.Iceberg and next_storage == TableStorage.S3:
            try:
                self.logger.info(f"Creating Iceberg catalog {catalog_name}")
                next_catalog_impl.create_namespace("test")
            except Exception:
                pass  # already exists
        elif next_lake == LakeFormat.DeltaLake and next_storage == TableStorage.S3:
            try:
                self.logger.info(f"Creating Delta Lake catalog {catalog_name}")
                self.run_unity_cmd(
                    ["schema", "create", "--catalog", catalog_name, "--name", "test"]
                )
            except Exception:
                pass  # already exists
        else:
            next_session = self.get_next_session(
                cluster, catalog_name, next_storage, next_lake, next_catalog
            )
            try:
                self.create_database(next_session, catalog_name)
            except Exception as e:
                saved_exception = e
            next_session.stop()
            if saved_exception is not None:
                raise saved_exception
        return True

    def create_lake_table(self, cluster, data) -> bool:
        saved_exception = None
        if data["engine"] == "kafka":
            # At the moment, this is an ugly hack
            return self.kafka_handler.create_kafka_table(
                cluster,
                data["database_name"],
                data["table_name"],
                data["topic"],
                data["format"],
                data["columns"],
            )
        catalog_name = data["catalog_name"]
        next_storage = TableStorage.storage_from_str(data["storage"])
        next_lake = LakeFormat.lakeformat_from_str(data["engine"])
        next_table_generator = LakeTableGenerator.get_next_generator(next_lake)
        catalog_type = LakeCatalogs.NoCatalog
        catalog_impl = None

        self.catalogs_lock.acquire()
        if catalog_name not in self.catalogs:
            self.catalogs[catalog_name] = DolorCatalog(
                catalog_name,
                LakeCatalogs.NoCatalog,
                None,
            )
            self.catalogs_lock.release()
            next_session = self.get_next_session(
                cluster, catalog_name, next_storage, next_lake, LakeCatalogs.NoCatalog
            )
            saved_exception = None
            try:
                self.create_database(next_session, catalog_name)
            except Exception as e:
                saved_exception = e
            next_session.stop()
            if saved_exception is not None:
                raise saved_exception
        else:
            catalog_type = self.catalogs[catalog_name].catalog_type
            catalog_impl = self.catalogs[catalog_name].catalog_impl
            self.catalogs_lock.release()

        next_sql, next_table = next_table_generator.generate_create_table_ddl(
            catalog_name,
            data["database_name"],
            data["table_name"],
            data["columns"],
            data["format"],
            data["deterministic"] > 0,
            next_storage,
            catalog_type,
        )
        next_session = self.get_next_session(
            cluster,
            catalog_name,
            next_storage,
            next_lake,
            catalog_type,
        )
        with self.catalogs_lock:
            self.catalogs[catalog_name].spark_tables[data["table_name"]] = next_table

        try:
            if (
                next_lake == LakeFormat.Iceberg
                and next_storage == TableStorage.S3
                and catalog_type
                in (LakeCatalogs.Hive, LakeCatalogs.REST, LakeCatalogs.Glue)
            ):
                next_info = next_table_generator.create_catalog_table(
                    catalog_impl, data["columns"], next_table
                )
                self.logger.info(f"Created catalog table: {next_info}")
            elif catalog_type == LakeCatalogs.NoCatalog:
                self.run_query(next_session, next_sql)
            else:
                raise Exception("I have not implemented this case yet")

            if random.randint(1, 5) != 5:
                self.data_generator.insert_random_data(next_session, next_table)
        except Exception as e:
            saved_exception = e
        next_session.stop()
        if saved_exception is not None:
            raise saved_exception
        return True

    def update_or_check_table(self, cluster, data) -> bool:
        res = False
        next_table = None
        next_session = None
        saved_exception = None
        catalog_name = data["catalog_name"]
        catalog_type = LakeCatalogs.NoCatalog
        run_background_worker = data["async"] == 0 and random.randint(1, 2) == 1

        if data["engine"] != "kafka":
            with self.catalogs_lock:
                next_table = self.catalogs[catalog_name].spark_tables[
                    data["table_name"]
                ]
                catalog_type = self.catalogs[catalog_name].catalog_type

        if run_background_worker:

            def my_new_task():
                client = Client(
                    host=(
                        cluster.instances["node0"].ip_address
                        if hasattr(cluster, "instances")
                        else "localhost"
                    ),
                    port=9000,
                    command=cluster.client_bin_path,
                )
                nloops = random.randint(1, 50)
                tbl = (
                    f"{data['catalog_name']}.{data['table_name']}"
                    if data["engine"] == "kafka"
                    else next_table.get_clickhouse_path()
                )
                for _ in range(nloops):
                    client.query(f"SELECT * FROM {tbl} LIMIT 100;")
                    time.sleep(1)

            self.worker.set_task_function(my_new_task)
            self.worker.resume()

        if data["engine"] != "kafka":
            next_session = self.get_next_session(
                cluster,
                catalog_name,
                next_table.storage,
                next_table.lake_format,
                catalog_type,
            )
        try:
            if data["engine"] == "kafka":
                res = self.kafka_handler.update_table(
                    cluster, data["catalog_name"], data["table_name"]
                )
            elif data["engine"] in ["iceberg", "deltalake"]:
                res = (
                    self.data_generator.update_table(next_session, next_table)
                    if random.randint(1, 10) < 9
                    else self.table_check.check_table(cluster, next_session, next_table)
                )
        except Exception as e:
            saved_exception = e
        if run_background_worker:
            self.worker.pause()
        if next_session is not None:
            next_session.stop()
        if saved_exception is not None:
            raise saved_exception
        return res

    def close_sessions(self):
        # Stop Unity catalog server
        with self.catalogs_lock:
            if self.uc_server is not None and self.uc_server.poll() is None:
                self.uc_server.kill()
                self.uc_server.wait()
        # Stop Spark session
        with self.spark_lock:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark:
                spark.stop()
