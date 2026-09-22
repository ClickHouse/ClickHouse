import pytest

from helpers.iceberg_utils import get_uuid_str
from .external_paths_utils import (
    ALL_ROWS,
    create_and_upload_table,
    _distribute_table_components,
    get_query_args,
)


@pytest.mark.parametrize("metadata_storage,manifest_list_storage,manifest_storage,data_storage", [
    ("s3", "local", "s3", "s3"),
    ("s3", "s3", "local", "s3"),
    ("s3", "s3", "s3", "local"),
    ("azure", "local", "azure", "azure"),
    ("azure", "azure", "local", "azure"),
    ("azure", "azure", "azure", "local"),
])
def test_multi_storage_combinations(started_cluster_iceberg_with_spark, metadata_storage, manifest_list_storage, manifest_storage, data_storage):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = f"test_combo_{get_uuid_str()}"

    create_and_upload_table(started_cluster_iceberg_with_spark, TABLE_NAME)

    base_path = _distribute_table_components(started_cluster_iceberg_with_spark, TABLE_NAME, metadata_storage,
                                             manifest_list_storage, manifest_storage, data_storage)

    func = {"s3": "icebergS3", "azure": "icebergAzure", "local": "icebergLocal"}[metadata_storage.split(":")[0]]
    args = get_query_args(metadata_storage, started_cluster_iceberg_with_spark, base_path)

    assert instance.query(f"SELECT * FROM {func}({args}) ORDER BY id") == ALL_ROWS
