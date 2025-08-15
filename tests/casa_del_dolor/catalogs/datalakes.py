import pyspark
import random

from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    NestedField,
    StringType,
)

from tests.casa_del_dolor.catalogs.laketables import (
    IcebergTableGenerator,
    DeltaLakePropertiesGenerator,
)
from integration.helpers.config_cluster import minio_secret_key
from integration.helpers.iceberg_utils import default_upload_directory

DEFAULT_SCHEMA = Schema(
    NestedField(field_id=1, name="c0", field_type=StringType(), required=False),
)

DEFAULT_PARTITION_SPEC = PartitionSpec()

DEFAULT_SORT_ORDER = SortOrder(SortField(source_id=1, transform=IdentityTransform()))


class Catalog:
    catalog = None
    catalog_type = str

    def __init__(self, _catalog, _catalog_type: str):
        self.catalog = _catalog
        self.catalog_type = _catalog_type


class LosCatalogos:
    catalogos: dict[str, Catalog] = {}

    def __init__(self):
        pass

    def create_catalog(
        self, cluster, catalog_name: str, catalog_type: str, catalog_uri: str
    ):
        cat_params = {
            "s3.endpoint": f"http://{cluster.minio_host}:{cluster.minio_port}",
            "s3.access-key-id": "minio",
            "s3.secret-access-key": minio_secret_key,
        }
        if catalog_type == "glue":
            cat_params["glue.endpoint"] = catalog_uri
            cat_params["glue.region"] = "us-east-1"
        else:
            cat_params["uri"] = catalog_uri
            cat_params["type"] = catalog_type
        self.catalogos[catalog_name] = Catalog(
            catalog_type, load_catalog(catalog_name, **cat_params)
        )

    def create_namespace(self, catalog_name: str, namespace_name: str):
        self.catalogos[catalog_name].catalog.create_namespace(namespace_name)

    def create_catalog_table(
        self,
        catalog_name: str,
        namespace_name: str,
        table_name: str,
        schema=DEFAULT_SCHEMA,
        partition_spec=DEFAULT_PARTITION_SPEC,
        sort_order=DEFAULT_SORT_ORDER,
        dir="data",
    ):
        return self.catalogos[catalog_name].catalog.create_table(
            identifier=f"{namespace_name}-{table_name}",
            schema=schema,
            location=f"s3://warehouse-{"hms" if self.catalogos[catalog_name].catalog_type == "hive" else self.catalogos[catalog_name].catalog_type}/{dir}",
            partition_spec=partition_spec,
            sort_order=sort_order,
        )


def get_spark(lake_type: str):
    spark_properties = {
        "iceberg": [
            ("spark.sql.iceberg.check-committer-thread", ["true", "false"]),
            ("spark.sql.iceberg.compress-metadata", ["true", "false"]),
            ("spark.sql.iceberg.vectorization.enabled", ["true", "false"]),
            ("spark.sql.iceberg.use-native-partition-data", ["true", "false"]),
            ("spark.sql.iceberg.planning.max-table-cache-size", ["10", "50", "100"]),
            ("spark.sql.iceberg.handle-timestamp-without-timezone", ["true", "false"]),
            ("spark.sql.iceberg.merge-on-read.enabled", ["true", "false"]),
            ("spark.sql.iceberg.write.distribution.mode", ["none", "hash", "range"]),
            ("spark.sql.iceberg.write.fanout.enabled", ["true", "false"]),
        ],
        "delta": [
            (
                "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",
                ["true", "false"],
            ),
            (
                "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact",
                ["true", "false"],
            ),
            (
                "spark.databricks.delta.retentionDurationCheck.enabled",
                ["true", "false"],
            ),
            ("spark.databricks.delta.vacuum.parallelDelete.enabled", ["true", "false"]),
            ("spark.databricks.delta.stalenessLimit", ["1h", "6h", "12h", "24h"]),
            ("spark.databricks.delta.metricsCollection.enabled", ["true", "false"]),
            (
                "spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled",
                ["true", "false"],
            ),
            (
                "spark.databricks.delta.optimize.maxFileSize",
                ["134217728", "268435456", "1073741824"],
            ),
            (
                "spark.databricks.delta.optimize.minFileSize",
                ["10485760", "20971520", "104857600"],
            ),
            ("spark.databricks.delta.optimize.repartition.enabled", ["true", "false"]),
            ("spark.databricks.delta.schema.autoMerge.enabled", ["true", "false"]),
            ("spark.databricks.delta.checkLatestSchemaOnRead", ["true", "false"]),
        ],
    }

    builder = pyspark.sql.SparkSession.builder.appName(f"spark_{lake_type}")

    if lake_type == "iceberg":
        builder = (
            builder.config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog",
            )
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", "/iceberg_data")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
        )
    elif lake_type == "delta":
        builder = builder.config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        ).config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")

    # Additional Spark-specific Iceberg settings
    next_properties = spark_properties[lake_type]
    for _ in range(min(0, len(next_properties))):
        idx = random.randint(0, len(next_properties) - 1)
        prop, values = next_properties.pop(idx)
        builder.config(prop, random.choice(values))

    return builder.master("local").getOrCreate()


def execute_spark_query(
    started_cluster, lake_type: str, storage_type: str, table_path: str, query: str
):
    started_cluster.spark_session[lake_type].sql(query)
    default_upload_directory(started_cluster, storage_type, table_path, table_path)


def generate_random_create_query(
    table_name: str, lake_type: str, columns: list[str]
) -> str:
    generator = (
        IcebergTableGenerator()
        if lake_type == "iceberg"
        else DeltaLakePropertiesGenerator()
    )

    return generator.generate_create_table_ddl(table_name, columns, lake_type)
