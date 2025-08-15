from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    NestedField,
    StringType,
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


def execute_spark_query(started_cluster, storage_type: str, tablepath: str, query: str):
    started_cluster.spark_session.sql(query)
    default_upload_directory(started_cluster, storage_type, tablepath, tablepath)
