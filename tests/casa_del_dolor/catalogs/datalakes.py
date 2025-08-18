import pyspark
import random

from tests.casa_del_dolor.catalogs.laketables import (
    TableStorage,
    TableFormat,
    LakeCatalogs,
    LakeTableGenerator,
)
from integration.helpers.config_cluster import minio_secret_key

# from integration.helpers.iceberg_utils import default_upload_directory
from ..properties import sample_from_dict


def get_spark(
    cluster,
    with_s3: bool,
    with_azure: bool,
    with_glue: bool,
    with_hadoop: bool,
    with_hive: bool,
    with_rest: bool,
    with_nessie: bool,
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
        "spark.sql.iceberg.planning.max-table-cache-size": lambda: random.choice(
            ["10", "50", "100"]
        ),
        "spark.sql.iceberg.use-native-partition-data": true_false_lambda,
        "spark.sql.iceberg.vectorization.enabled": true_false_lambda,
        "spark.sql.iceberg.write.distribution.mode": lambda: random.choice(
            ["none", "hash", "range"]
        ),
        "spark.sql.iceberg.write.fanout.enabled": true_false_lambda,
        "spark.sql.parquet.compression.codec": lambda: random.choice(["snappy"]),
        "spark.sql.parquet.filterPushdown": true_false_lambda,
        "spark.sql.parquet.mergeSchema": true_false_lambda,
    }

    builder = pyspark.sql.SparkSession.builder.appName(f"spark_test")

    # ============================================================
    # CORE CONFIGURATIONS FOR BOTH ICEBERG AND DELTA
    # ============================================================
    # Enable both Iceberg and Delta extensions
    builder.config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.iceberg.spark.SparkSessionCatalog",
    )
    builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    # Default catalog remains as Delta-compatible
    builder.config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")

    builder.config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    # This is the default storage
    builder.config(
        "spark.sql.catalog.local.warehouse", "/var/lib/clickhouse/user_files/lakehouse"
    )

    # ============================================================
    # CATALOG CONFIGURATIONS
    # ============================================================
    if with_glue:
        builder.config(
            "spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog"
        )
        builder.config(
            "spark.sql.catalog.glue.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        builder.config(
            "spark.sql.catalog.glue.warehouse",
            f"http://{cluster.minio_host}:{cluster.minio_port}/warehouse-glue",
        )
        builder.config(
            "spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
    if with_hadoop:
        # Iceberg catalog with Hadoop (works for both Azure and S3)
        builder.config(
            "spark.sql.catalog.hadoop", "org.apache.iceberg.spark.SparkCatalog"
        )
        builder.config("spark.sql.catalog.hadoop.type", "hadoop")
        builder.config(
            "spark.sql.catalog.hadoop.warehouse",
            f"http://{cluster.minio_host}:{cluster.minio_port}/warehouse-hadoop",
        )
    if with_hive:
        builder.config(
            "spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog"
        )
        builder.config("spark.sql.catalog.hive.type", "hive")
        builder.config("spark.sql.catalog.hive.uri", "thrift://hive:9083")
        builder.config(
            "spark.sql.catalog.hive.warehouse",
            f"http://{cluster.minio_host}:{cluster.minio_port}/warehouse-hms",
        )
        builder.enableHiveSupport()
    if with_rest:
        builder.config(
            "spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog"
        )
        builder.config(
            "spark.sql.catalog.rest.catalog-impl", "org.apache.iceberg.rest.RESTCatalog"
        )
        builder.config("spark.sql.catalog.rest.uri", "http://rest:8181")
        # Warehouse location (S3, Azure, or local)
        builder.config(
            "spark.sql.catalog.rest.warehouse",
            f"http://{cluster.minio_host}:{cluster.minio_port}/warehouse-rest",
        )
        # S3 configurations for REST catalog
        builder.config(
            "spark.sql.catalog.rest.s3.endpoint",
            f"http://{cluster.minio_host}:{cluster.minio_port}",
        )
        # IO implementation for REST catalog
        builder.config(
            "spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        builder.config("spark.sql.catalog.rest.s3.access-key-id", "minio")
        builder.config("spark.sql.catalog.rest.s3.secret-access-key", minio_secret_key)
        builder.config("spark.sql.catalog.rest.s3.path-style-access", "true")
    if with_nessie:
        builder.config(
            "spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog"
        )
        builder.config(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        # builder.config("spark.sql.catalog.nessie.uri", nessie_uri)
        # builder.config("spark.sql.catalog.nessie.ref", nessie_ref)
        # if nessie_auth_token:
        #    builder.config("spark.sql.catalog.nessie.authentication.type", "BEARER")
        #    builder.config("spark.sql.catalog.nessie.authentication.token", nessie_auth_token)
        # Warehouse for Nessie
        builder.config(
            "spark.sql.catalog.nessie.warehouse",
            f"http://{cluster.minio_host}:{cluster.minio_port}/warehouse-nessie",
        )

    # ============================================================
    # AZURE CONFIGURATIONS
    # ============================================================
    if with_azure:
        builder.config(
            "spark.hadoop.fs.azure.account.key.devstoreaccount1.dfs.core.windows.net",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        )
        # Azure-specific optimizations
        builder.config("spark.hadoop.fs.azure.account.auth.type", "SharedKey")
        builder.config(
            "spark.hadoop.fs.abfs.impl",
            "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem",
        )
        builder.config(
            "spark.hadoop.fs.abfss.impl",
            "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
        )
        # Enable Delta Lake for Azure
        builder.config("spark.databricks.delta.storage.azure.enabled", "true")

    # ============================================================
    # AWS S3 CONFIGURATIONS
    # ============================================================
    if with_s3:
        builder.config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        # Using explicit credentials
        builder.config(
            "spark.hadoop.fs.s3a.endpoint",
            f"http://{cluster.minio_host}:{cluster.minio_port}",
        )
        builder.config("spark.hadoop.fs.s3a.access.key", minio_secret_key)
        builder.config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
        # S3 optimizations
        # builder.config("spark.hadoop.fs.s3a.fast.upload", "true")
        # builder.config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
        # builder.config("spark.hadoop.fs.s3a.multipart.size", "104857600")
        # builder.config("spark.hadoop.fs.s3a.multipart.threshold", "2147483647")
        # Enable Delta Lake with Glue
        if with_glue:
            builder.config("spark.databricks.delta.catalog.glue.enabled", "true")

    # Random properties
    if random.randint(1, 100) <= 70:
        selected_properties = sample_from_dict(
            spark_properties, random.randint(0, len(spark_properties))
        )
        for key, val in selected_properties.items():
            builder.config(key, val())

    return builder.getOrCreate()


def create_lake_table(
    cluster,
    table_name: str,
    storage_type: str,
    format: str,
    catalog: str,
    columns: list[dict[str, str]],
):
    next_storage = TableStorage.storage_from_str(storage_type)
    next_format = TableFormat.format_from_str(format)
    next_catalog = LakeCatalogs.catalog_from_str(catalog)
    next_generator = LakeTableGenerator.get_next_generator(
        cluster.minio_bucket, next_format
    )
    next_query = next_generator.generate_create_table_ddl(
        table_name, columns, next_storage, next_catalog
    )
    cluster.spark_session.sql(next_query)
    # if next_catalog == LakeCatalogs.NoCatalog:
    #    default_upload_directory(cluster, storage_type, f"/var/lib/clickhouse/user_files/lakehouse/default/{table_name}/", f"/var/lib/clickhouse/user_files/lakehouse/default/{table_name}/")
