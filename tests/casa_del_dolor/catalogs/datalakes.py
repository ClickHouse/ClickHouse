import logging
import os
import random
import typing
import urllib3

from minio import Minio
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
from .laketables import (
    TableStorage,
    TableFormat,
    LakeCatalogs,
    LakeTableGenerator,
)
from integration.helpers.config_cluster import minio_access_key, minio_secret_key

# from integration.helpers.iceberg_utils import default_upload_directory

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
azure_account_name: str = "devstoreaccount1"
azure_account_key: str = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)
azure_container: str = "cont"


def get_local_base_path(catalog_name: str) -> str:
    return f"/var/lib/clickhouse/user_files/lakehouse/{catalog_name}"


Parameter = typing.Callable[[], int | float]


def sample_from_dict(d: dict[str, Parameter], sample: int) -> dict[str, Parameter]:
    items = random.sample(list(d.items()), sample)
    return dict(items)


def get_spark(
    cluster,
    catalog_name: str,
    storage: TableStorage,
    format: TableFormat,
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
        "org.apache.spark:spark-hadoop-cloud_2.12:3.5.2",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
        "org.apache.iceberg:iceberg-spark-extensions-3.5_2.12:1.9.2",
        "org.apache.iceberg:iceberg-aws-bundle:1.9.2",
        "org.apache.iceberg:iceberg-azure-bundle:1.9.2",
        "io.delta:delta-spark_2.12:3.3.2",
    ]
    nessie_jars = [
        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.4_2.12:0.76.0",
        "org.projectnessie:nessie-spark-runtime-3.4_2.12:0.76.0",
    ]

    if format == TableFormat.Iceberg:
        catalog_extension = (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        catalog_format = "org.apache.iceberg.spark.SparkCatalog"
    elif format == TableFormat.DeltaLake:
        catalog_extension = "io.delta.sql.DeltaSparkSessionExtension"
        catalog_format = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    else:
        raise Exception("Unknown format")

    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {",".join(all_jars)} pyspark-shell"

    from pyspark.sql import SparkSession

    builder = SparkSession.builder
    builder.config("spark.sql.extensions", catalog_extension)
    builder.config(f"spark.sql.catalog.{catalog_name}", catalog_format)

    # ============================================================
    # CATALOG CONFIGURATIONS
    # ============================================================
    if catalog == LakeCatalogs.Glue:
        builder.config(
            f"spark.sql.catalog.{catalog_name}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
    elif catalog == LakeCatalogs.Hive:
        # Enable Hive support
        builder.config(f"spark.sql.catalog.{catalog_name}.type", "hive")
        builder.config("spark.sql.catalogImplementation", "hive")
        builder.config(f"spark.sql.catalog.{catalog_name}.uri", "thrift://0.0.0.0:9083")
        # Hive metastore version
        builder.config("spark.sql.hive.metastore.version", "3.1.3")
        builder.config("spark.sql.hive.metastore.jars", "builtin")
        # Schema handling
        builder.config("spark.sql.hive.metastore.schema.verification", "false")
        builder.config("spark.sql.hive.metastore.schema.verification.record", "false")
        # Partitioning
        builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        builder.config("spark.sql.hive.convertMetastoreParquet", "true")
        builder.config("spark.sql.hive.convertMetastoreOrc", "true")
        # Glue metastore?
        builder.config(
            "spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )
        builder.enableHiveSupport()
    elif catalog == LakeCatalogs.REST:
        builder.config(
            f"spark.sql.catalog.{catalog_name}.catalog-impl",
            "org.apache.iceberg.rest.RESTCatalog",
        )
        builder.config(f"spark.sql.catalog.{catalog_name}.uri", "http://localhost:8182")
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
    elif catalog == LakeCatalogs.Nessie:
        builder.config(
            f"spark.sql.catalog.{catalog_name}.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        # builder.config(f"spark.sql.catalog.{catalog_name}.uri", "uri")
        # builder.config(f"spark.sql.catalog.{catalog_name}.ref", "ref")
    else:
        builder.config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")

    # ============================================================
    # STORAGE CONFIGURATIONS
    # ============================================================
    if storage == TableStorage.S3:
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
        builder.config("spark.hadoop.fs.s3a.region", "us-east-1")
        builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        if catalog == LakeCatalogs.Glue:
            if format == TableFormat.DeltaLake:
                builder.config("spark.databricks.delta.catalog.glue.enabled", "true")
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
        elif catalog == LakeCatalogs.Hive:
            builder.config("spark.hadoop.aws.region", "us-east-1")

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
        builder.config(
            f"spark.hadoop.fs.azure.account.key.{azure_account_name}.blob.core.windows.net",
            azure_account_key,
        )
        # For Azurite local emulation
        builder.config(
            f"spark.hadoop.fs.azure.storage.emulator.account.name", azure_account_name
        )
        builder.config(
            f"spark.hadoop.fs.azure.account.key.{azure_account_name}.dfs.core.windows.net",
            azure_account_key,
        )
        # Override endpoints for Azurite
        builder.config(
            f"spark.hadoop.fs.azure.account.blob.endpoint.{azure_account_name}.blob.core.windows.net",
            f"http://azurite1:{cluster.azurite_port}/{azure_account_name}",
        )
        # WASB implementation, ABFS is not compatible with Azurite?
        builder.config(
            "spark.hadoop.fs.wasb.impl",
            "org.apache.hadoop.fs.azure.NativeAzureFileSystem",
        )
        if format == TableFormat.DeltaLake:
            # Enable Delta Lake for Azure
            builder.config("spark.databricks.delta.storage.azure.enabled", "true")

        builder.config(
            "spark.sql.warehouse.dir",
            f"wasb://{azure_container}@{azure_account_name}/{catalog_name}",
        )
        builder.config(
            f"spark.sql.catalog.{catalog_name}.warehouse",
            f"wasb://{azure_container}@{azure_account_name}/{catalog_name}",
        )
    elif storage == TableStorage.Local:
        os.makedirs(get_local_base_path(catalog_name), exist_ok=True)

        builder.config(
            "spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem"
        )
        builder.config(
            "spark.sql.warehouse.dir", f"file://{get_local_base_path(catalog_name)}"
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

    # Random properties
    # if random.randint(1, 100) <= 70:
    #    selected_properties = sample_from_dict(
    #        spark_properties, random.randint(0, len(spark_properties))
    #    )
    #    for key, val in selected_properties.items():
    #        builder.config(key, val())

    return builder.getOrCreate()


class DolorCatalog:
    def __init__(
        self,
        cluster,
        _catalog_name: str,
        _storage_type: TableStorage,
        _format_type: TableFormat,
        _catalog_type: LakeCatalogs,
    ):
        self.catalog_name = _catalog_name
        self.storage_type = _storage_type
        self.format_type = _format_type
        self.catalog_type = _catalog_type
        self.session = get_spark(
            cluster, _catalog_name, _storage_type, _format_type, _catalog_type
        )


class SparkHandler:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.catalogs = {}

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

    def create_database(self, session, catalog_name: str):
        next_sql = f"CREATE DATABASE IF NOT EXISTS {catalog_name}.test;"
        self.logger.info(f"Running query: {next_sql}")
        session.sql(next_sql)

    def create_lake_database(
        self,
        cluster,
        catalog_name: str,
        storage_type: str,
        format: str,
        catalog: str,
    ):
        next_storage = TableStorage.storage_from_str(storage_type)
        next_format = TableFormat.format_from_str(format)
        next_catalog = LakeCatalogs.catalog_from_str(catalog)

        if next_catalog == LakeCatalogs.REST:
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
                        "wasb.account-name": azure_account_name,
                        "wasb.account-key": azure_account_key,
                        "azure.blob-endpoint": f"http://azurite1:{cluster.azurite_port}/{azure_account_name}",
                    }
                )
                next_warehouse = f"wasb://{azure_container}@{azure_account_name}"
            elif next_storage == TableStorage.Local:
                next_warehouse = f"file://{get_local_base_path(catalog_name)}"
            rest_catalog = RestCatalog(
                catalog_name,
                uri="http://localhost:8182",
                warehouse=next_warehouse,
                **kwargs,
            )
            rest_catalog.create_namespace("test")
        elif next_catalog == LakeCatalogs.Glue:
            if next_storage == TableStorage.S3:
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
                raise Exception("Not possible at the moment")
        elif next_catalog == LakeCatalogs.Hive:
            if next_storage == TableStorage.S3:
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
                raise Exception("Not possible at the moment")
        elif next_catalog == LakeCatalogs.Nessie:
            raise Exception("Not possible at the moment")

        self.catalogs[catalog_name] = DolorCatalog(
            cluster, catalog_name, next_storage, next_format, next_catalog
        )
        self.create_database(self.catalogs[catalog_name].session, catalog_name)

    def create_lake_table(
        self,
        cluster,
        catalog_name: str,
        table_name: str,
        storage_type: str,
        format: str,
        columns: list[dict[str, str]],
    ):
        next_session = None
        next_storage = TableStorage.storage_from_str(storage_type)
        next_format = TableFormat.format_from_str(format)
        next_generator = LakeTableGenerator.get_next_generator(
            cluster.minio_bucket, next_format
        )

        if catalog_name[0] != "d":
            next_session = get_spark(
                cluster, catalog_name, next_storage, next_format, LakeCatalogs.NoCatalog
            )
            self.create_database(next_session, catalog_name)
        else:
            next_session = cluster.catalogs[catalog_name].session
        next_sql = next_generator.generate_create_table_ddl(
            catalog_name, table_name, columns
        )
        self.logger.info(f"Running query: {next_sql}")
        next_session.sql(next_sql)
