import logging
import os
import random
import shutil
import socket
import subprocess
import time
import typing
import urllib3

from pathlib import Path
from minio import Minio
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
from .laketables import (
    TableStorage,
    LakeFormat,
    LakeCatalogs,
    SparkTable,
)
from .tablegenerator import LakeTableGenerator
from .datagenerator import LakeDataGenerator

from integration.helpers.config_cluster import minio_access_key, minio_secret_key

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


def get_local_base_path(cluster, catalog_name: str) -> str:
    return f"{Path(cluster.instances_dir) / "node0" / "database" / "user_files" / "lakehouse" / f"{catalog_name}"}"


Parameter = typing.Callable[[], int | float]


def sample_from_dict(d: dict[str, Parameter], sample: int) -> dict[str, Parameter]:
    items = random.sample(list(d.items()), sample)
    return dict(items)


def get_spark(
    cluster,
    logfile: str,
    catalog_name: str,
    storage: TableStorage,
    lake: LakeFormat,
    catalog: LakeCatalogs,
):
    true_false_lambda = lambda: random.choice(["false", "true"])

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
        "spark.databricks.delta.retentionDurationCheck.enabled": true_false_lambda,
        "spark.databricks.delta.schema.autoMerge.enabled": true_false_lambda,
        "spark.databricks.delta.stalenessLimit": lambda: random.choice(
            ["1h", "6h", "12h", "24h"]
        ),
        "spark.databricks.delta.vacuum.parallelDelete.enabled": true_false_lambda,
        "spark.sql.adaptive.coalescePartitions.enabled": true_false_lambda,
        "spark.sql.adaptive.enabled": true_false_lambda,
        "spark.sql.adaptive.localShuffleReader.enabled": true_false_lambda,
        "spark.sql.adaptive.skewJoin.enabled": true_false_lambda,
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
        "spark.sql.orc.compression.codec": lambda: random.choice(["snappy"]),
        "spark.sql.parquet.compression.codec": lambda: random.choice(["snappy"]),
        "spark.sql.parquet.filterPushdown": true_false_lambda,
        "spark.sql.parquet.mergeSchema": true_false_lambda,
        "spark.sql.statistics.autogather": true_false_lambda,
    }

    # ============================================================
    # CORE CONFIGURATIONS FOR BOTH ICEBERG AND DELTA
    # ============================================================
    catalog_extension = ""
    catalog_format = ""
    all_jars = [
        "org.apache.spark:spark-hadoop-cloud_2.12:3.5.6",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
        "org.apache.iceberg:iceberg-spark-extensions-3.5_2.12:1.9.2",
        "org.apache.iceberg:iceberg-aws:1.9.2",
        "org.apache.iceberg:iceberg-azure-bundle:1.9.2",
        "io.delta:delta-spark_2.12:3.3.2",
        "io.unitycatalog:unitycatalog-spark_2.12:0.2.0",
        "com.microsoft.azure:azure-storage:8.6.6",
        "org.apache.hadoop:hadoop-azure:3.3.6",
    ]

    nessie_jars = [
        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.4_2.12:0.76.0",
        "org.projectnessie:nessie-spark-runtime-3.4_2.12:0.76.0",
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

    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {",".join(all_jars)} pyspark-shell"

    from pyspark.sql import SparkSession

    builder = SparkSession.builder
    builder.config("spark.sql.extensions", catalog_extension)
    builder.config(f"spark.sql.catalog.{catalog_name}", catalog_format)
    builder.config(
        "spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile=file:{logfile}"
    )
    builder.config(
        "spark.executor.extraJavaOptions", f"-Dlog4j.configurationFile=file:{logfile}"
    )

    # ============================================================
    # CATALOG CONFIGURATIONS
    # ============================================================
    if catalog == LakeCatalogs.Glue:
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
    elif catalog == LakeCatalogs.Hive:
        # Enable Hive support
        if lake == LakeFormat.Iceberg:
            builder.config(f"spark.sql.catalog.{catalog_name}.type", "hive")
            builder.config(
                f"spark.sql.catalog.{catalog_name}.uri", "thrift://0.0.0.0:9083"
            )
        # Hive metastore version
        builder.config("spark.sql.hive.metastore.version", "3.1.3")
        builder.config("spark.sql.hive.metastore.jars", "maven")
        builder.enableHiveSupport()
    elif catalog == LakeCatalogs.REST or (
        catalog == LakeCatalogs.Unity and lake == LakeFormat.Iceberg
    ):
        builder.config(
            f"spark.sql.catalog.{catalog_name}.catalog-impl",
            "org.apache.iceberg.rest.RESTCatalog",
        )
        builder.config(
            f"spark.sql.catalog.{catalog_name}.uri",
            f"http://localhost:{"8081/api/2.1/unity-catalog/iceberg" if catalog == LakeCatalogs.Unity else "8182"}",
        )
        builder.config(
            f"spark.sql.catalog.{catalog_name}.cache-enabled",
            random.choice(["true", "false"]),
        )

        if storage == TableStorage.S3:
            builder.config(
                f"spark.sql.catalog.{catalog_name}.s3.endpoint",
                f"http://{cluster.minio_ip}:{cluster.minio_port}",
            )
            builder.config(
                f"spark.sql.catalog.{catalog_name}.s3.access-key-id", "minio"
            )
            builder.config(
                f"spark.sql.catalog.{catalog_name}.s3.secret-access-key",
                minio_access_key,
            )
            builder.config(
                f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true"
            )
    elif catalog == LakeCatalogs.Unity and lake == LakeFormat.DeltaLake:
        builder.config(f"spark.sql.catalog.{catalog_name}.uri", "http://localhost:8081")
        builder.config(f"spark.sql.catalog.{catalog_name}.token", "")
        builder.config("spark.sql.defaultCatalog", f"{catalog_name}")
    elif catalog == LakeCatalogs.Nessie:
        builder.config(
            f"spark.sql.catalog.{catalog_name}.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        # builder.config(f"spark.sql.catalog.{catalog_name}.uri", "uri")
        # builder.config(f"spark.sql.catalog.{catalog_name}.ref", "ref")
    elif lake == LakeFormat.Iceberg:
        # Something as default for iceberg, nothing needed for delta
        builder.config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")

    # ============================================================
    # STORAGE CONFIGURATIONS
    # ============================================================
    if catalog == LakeCatalogs.Unity and lake == LakeFormat.Iceberg:
        # It has to point to unity
        builder.config("spark.sql.warehouse.dir", "unity")
        builder.config(f"spark.sql.catalog.{catalog_name}.warehouse", "unity")

        if storage == TableStorage.S3:
            builder.config(
                f"spark.sql.catalog.{catalog_name}.warehouse",
                f"s3a://{cluster.minio_bucket}/{catalog_name}",
            )
        elif storage == TableStorage.Azure:
            builder.config(
                f"spark.sql.catalog.{catalog_name}.warehouse",
                f"wasb://{cluster.azure_container_name}@{cluster.azurite_account}/{catalog_name}",
            )
        elif storage == TableStorage.Local:
            builder.config(
                f"spark.sql.catalog.{catalog_name}.warehouse",
                f"file://{get_local_base_path(cluster, catalog_name)}",
            )
    elif storage == TableStorage.S3:
        # S3A filesystem implementation
        builder.config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        # MinIO endpoint and credentials
        builder.config(
            "spark.hadoop.fs.s3a.endpoint",
            f"http://{cluster.minio_ip}:{cluster.minio_port}",
        )
        builder.config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        builder.config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        # MinIO specific settings
        builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
        builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        builder.config("spark.hadoop.fs.s3a.path-style-access", "true")
        builder.config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        if catalog == LakeCatalogs.Glue:
            builder.config(
                f"spark.sql.catalog.{catalog_name}.endpoint", "http://localhost:3000"
            )
            builder.config(f"spark.sql.catalog.{catalog_name}.region", "us-east-1")
            builder.config(
                f"spark.sql.catalog.{catalog_name}.access-key-id", minio_access_key
            )
            builder.config(
                f"spark.sql.catalog.{catalog_name}.secret-access-key", minio_secret_key
            )

        # S3 optimizations
        # builder.config("spark.hadoop.fs.s3a.fast.upload", "true")
        # builder.config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
        # builder.config("spark.hadoop.fs.s3a.multipart.size", "104857600")
        # builder.config("spark.hadoop.fs.s3a.multipart.threshold", "2147483647")

        builder.config(
            "spark.sql.warehouse.dir", f"s3a://{cluster.minio_bucket}/{catalog_name}"
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

        builder.config(
            "spark.sql.warehouse.dir",
            f"wasb://{cluster.azure_container_name}@{cluster.azurite_account}/{catalog_name}",
        )
        builder.config(
            f"spark.sql.catalog.{catalog_name}.warehouse",
            f"wasb://{cluster.azure_container_name}@{cluster.azurite_account}/{catalog_name}",
        )
    elif storage == TableStorage.Local:
        os.makedirs(get_local_base_path(cluster, catalog_name), exist_ok=True)

        builder.config(
            "spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem"
        )
        builder.config(
            "spark.sql.warehouse.dir",
            f"file://{get_local_base_path(cluster, catalog_name)}",
        )
        builder.config(
            f"spark.sql.catalog.{catalog_name}.warehouse",
            f"file://{get_local_base_path(cluster, catalog_name)}",
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

    # Random properties
    if random.randint(1, 100) <= 70:
        selected_properties = sample_from_dict(
            spark_properties, random.randint(0, len(spark_properties))
        )
        for key, val in selected_properties.items():
            builder.config(key, val())

    return builder.getOrCreate()


class DolorCatalog:
    def __init__(
        self,
        cluster,
        spark_log_config: str,
        _catalog_name: str,
        _storage_type: TableStorage,
        _lake_type: LakeFormat,
        _catalog_type: LakeCatalogs,
    ):
        self.catalog_name = _catalog_name
        self.storage_type = _storage_type
        self.lake_type = _lake_type
        self.catalog_type = _catalog_type
        self.session = get_spark(
            cluster,
            spark_log_config,
            _catalog_name,
            _storage_type,
            _lake_type,
            _catalog_type,
        )
        self.spark_tables: dict[str, SparkTable] = {}


def wait_for_port(host, port, timeout=90):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with socket.create_connection((host, port), 1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


class SparkHandler:
    def __init__(self, cluster, args):
        self.logger = logging.getLogger(__name__)
        self.uc_server = None
        self.catalogs = {}
        self.uc_server_dir = None
        if args.with_unity is not None:
            self.uc_server_dir = args.with_unity
            self.uc_server_run_dir = Path(cluster.instances_dir) / "ucserver"

        self.spark_log_dir = Path(cluster.instances_dir) / "spark"
        self.spark_log_config = Path(cluster.instances_dir) / "log4j2.properties"
        self.spark_query_logger = self.spark_log_dir / "query.log"
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
        with open(self.spark_log_config, "w") as f:
            f.write(spark_log)

    def create_minio_bucket(self, cluster, bucket_name: str):
        minio_client = Minio(
            f"{cluster.minio_ip}:{cluster.minio_port}",
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False,
            http_client=urllib3.PoolManager(cert_reqs="CERT_NONE"),
        )
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

    def start_uc_server(self):
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

            env = os.environ.copy()
            env["UC_HOST"] = "localhost"
            env["UC_PORT"] = "8081"
            env["UC_DIR"] = str(self.uc_server_dir)
            uc_server_bin = self.uc_server_dir / "bin" / "start-uc-server"
            os.makedirs(self.uc_server_run_dir, exist_ok=True)
            # Start the server
            with open(self.uc_server_run_dir / "uc_server_bin.log", "w") as f1:
                with open(self.uc_server_run_dir / "uc_server_bin.err.log", "w") as f2:
                    self.uc_server = subprocess.Popen(
                        [str(uc_server_bin)],
                        cwd=str(self.uc_server_run_dir),
                        env=env,
                        stdout=f1,
                        stderr=f2,
                        text=True,
                        bufsize=1,
                    )
            self.logger.info(f"Starting UC server using pid = {self.uc_server.pid}")
            if not wait_for_port(env["UC_HOST"], env["UC_PORT"], timeout=120):
                # Print a few lines of output to help debug
                try:
                    for _ in range(50):
                        line = self.uc_server.stdout.readline()
                        if not line:
                            break
                        self.logger.error(
                            f"UC server did not start: Here is a line {line}"
                        )
                except Exception:
                    pass
                raise TimeoutError(
                    f"UC server did not start on {env["UC_HOST"]}:{env["UC_PORT"]} within timeout"
                )
            self.logger.info(
                f"UC server is accepting connections on http://{env["UC_HOST"]}:{env["UC_PORT"]}"
            )

    def create_uc_namespace(self, catalog_name: str):
        if self.uc_server_dir is None:
            raise RuntimeError(
                "with_unity argument with path to unity server dir is required"
            )
        uc_server_bin = self.uc_server_dir / "bin" / "uc"
        cmd = [
            str(uc_server_bin),
            "schema",
            "create",
            "--catalog",
            "unity",
            "--name",
            catalog_name,
        ]
        self.logger.info(f"Creating unity namespace {catalog_name}")
        proc = subprocess.Popen(
            cmd, cwd=str(self.uc_server_run_dir), env=os.environ.copy(), text=True
        )
        if proc.returncode != 0:
            self.logger.error(
                f"UC CLI failed (exit {proc.returncode}).\n"
                f"Command: {' '.join(cmd)}\n"
                f"stdout:\n{proc.stdout}\n"
                f"stderr:\n{proc.stderr}"
            )

    def run_query(self, session, query: str):
        self.logger.info(f"Running query: {query}")
        with open(self.spark_query_logger, "a") as f:
            f.write(query + "\n")
        session.sql(query)

    def create_database(self, session, catalog_name: str):
        next_sql = f"CREATE DATABASE IF NOT EXISTS {catalog_name}.test;"
        self.run_query(session, next_sql)

    def create_lake_database(self, cluster, data):
        catalog = data["catalog"]
        catalog_name = data["database_name"]
        next_storage = TableStorage.storage_from_str(data["storage"])
        next_lake = LakeFormat.lakeformat_from_str(data["lake"])
        next_catalog = LakeCatalogs.catalog_from_str(catalog)

        # Load catalog if needed
        if next_catalog == LakeCatalogs.REST:
            if next_lake == LakeFormat.Iceberg:
                next_warehouse = ""
                kwargs = {"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"}
                if next_storage == TableStorage.S3:
                    kwargs.update(
                        {
                            "s3.endpoint": f"http://{cluster.minio_ip}:{cluster.minio_port}",
                            "s3.access-key-id": minio_access_key,
                            "s3.secret-access-key": minio_secret_key,
                            "s3.region": "us-east-1",
                            "s3.path-style-access": "true",
                        }
                    )
                    next_warehouse = f"s3a://{cluster.minio_bucket}/{catalog_name}"
                elif next_storage == TableStorage.Azure:
                    kwargs.update(
                        {
                            "wasb.account-name": cluster.azurite_account,
                            "wasb.account-key": cluster.azurite_key,
                            "azure.blob-endpoint": f"http://{cluster.azurite_ip}:{cluster.azurite_port}/{cluster.azurite_account}",
                        }
                    )
                    next_warehouse = f"wasb://{cluster.azure_container_name}@{cluster.azurite_account}"
                elif next_storage == TableStorage.Local:
                    next_warehouse = (
                        f"file://{get_local_base_path(cluster, catalog_name)}"
                    )
                rest_catalog = RestCatalog(
                    catalog_name,
                    uri="http://localhost:8182",
                    warehouse=next_warehouse,
                    **kwargs,
                )
                rest_catalog.create_namespace("test")
            else:
                raise Exception("REST catalog not avaiable outside Iceberg")
        elif next_catalog == LakeCatalogs.Glue:
            if next_storage == TableStorage.S3:
                if next_lake == LakeFormat.Iceberg:
                    glue_catalog = load_catalog(
                        catalog,
                        **{
                            "type": "glue",
                            "glue.endpoint": "http://localhost:3000",
                            "glue.region": "us-east-1",
                            "glue.access-key-id": minio_access_key,
                            "glue.secret-access-key": minio_secret_key,
                            "s3.endpoint": f"http://{cluster.minio_ip}:{cluster.minio_port}",
                            "s3.access-key-id": minio_access_key,
                            "s3.secret-access-key": minio_secret_key,
                            "s3.region": "us-east-1",
                            "s3.path-style-access": "true",
                        },
                    )
                    glue_catalog.create_namespace("test")
            else:
                raise Exception("Glue catalog not available outside AWS at the moment")
        elif next_catalog == LakeCatalogs.Hive:
            if next_storage == TableStorage.S3:
                if next_lake == LakeFormat.Iceberg:
                    hive_catalog = load_catalog(
                        catalog,
                        **{
                            "uri": "thrift://0.0.0.0:9083",
                            "type": "hive",
                            "s3.endpoint": f"http://{cluster.minio_ip}:{cluster.minio_port}",
                            "s3.access-key-id": minio_access_key,
                            "s3.secret-access-key": minio_secret_key,
                            "s3.region": "us-east-1",
                            "s3.path-style-access": "true",
                        },
                    )
                    hive_catalog.create_namespace("test")
            else:
                raise Exception("Hive catalog not available outside AWS at the moment")
        elif next_catalog == LakeCatalogs.Unity:
            self.start_uc_server()
            self.create_uc_namespace(catalog_name)
        elif next_catalog == LakeCatalogs.Nessie:
            raise Exception("No Nessie yet")

        self.catalogs[catalog_name] = DolorCatalog(
            cluster,
            str(self.spark_log_config),
            catalog_name,
            next_storage,
            next_lake,
            next_catalog,
        )
        self.create_database(self.catalogs[catalog_name].session, catalog_name)

    def create_lake_table(self, cluster, data):
        catalog_name = data["database_name"]
        one_time = catalog_name[0] != "d"
        next_session = None
        next_storage = TableStorage.storage_from_str(data["storage"])
        next_lake = LakeFormat.lakeformat_from_str(data["lake"])
        next_table_generator = LakeTableGenerator.get_next_generator(
            cluster.minio_bucket, next_lake
        )

        if one_time:
            self.catalogs[catalog_name] = DolorCatalog(
                cluster,
                str(self.spark_log_config),
                catalog_name,
                next_storage,
                next_lake,
                LakeCatalogs.NoCatalog,
            )
            self.create_database(self.catalogs[catalog_name].session, catalog_name)
        next_session = self.catalogs[catalog_name].session
        next_sql, next_table = next_table_generator.generate_create_table_ddl(
            catalog_name, data["table_name"], data["columns"], data["format"]
        )
        self.run_query(next_session, next_sql)
        self.catalogs[catalog_name].spark_tables[data["table_name"]] = next_table

        self.logger.info(
            f"Inserting data into {data["table_name"]} in catalog: {catalog_name}"
        )
        next_data_generator = LakeDataGenerator()
        next_data_generator.insert_random_data(next_session, catalog_name, next_table)

        if one_time:
            next_session.stop()

    def close_sessions(self):
        for _, val in self.catalogs.items():
            val.session.stop()
        if self.uc_server is not None and self.uc_server.poll() is None:
            self.uc_server.kill()
            self.uc_server.wait()
