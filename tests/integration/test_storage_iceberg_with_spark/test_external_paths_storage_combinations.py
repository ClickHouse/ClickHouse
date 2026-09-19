import pytest

from helpers.iceberg_utils import default_upload_directory, get_uuid_str
from .external_paths_utils import (
    VALID_COMBINATIONS,
    _distribute_table_components,
    get_query_args,
    get_table_function,
)


@pytest.mark.parametrize("metadata_storage,manifest_list_storage,manifest_storage,data_storage", VALID_COMBINATIONS)
def test_multi_storage_combinations(started_cluster_iceberg_with_spark, metadata_storage, manifest_list_storage, manifest_storage, data_storage):
    """
    Test Iceberg table with all components in different storage locations.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = f"test_combo_{get_uuid_str()}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id INT, value STRING) USING iceberg OPTIONS('format-version'='2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_upload_directory(started_cluster_iceberg_with_spark, "s3", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/")

    base_path = _distribute_table_components(started_cluster_iceberg_with_spark, TABLE_NAME, metadata_storage,
                                             manifest_list_storage, manifest_storage, data_storage)

    func = get_table_function(metadata_storage)
    args = get_query_args(metadata_storage, started_cluster_iceberg_with_spark, base_path)

    assert instance.query(f"SELECT * FROM {func}({args}) ORDER BY id") == "1\talpha\n2\tbeta\n3\tgamma\n"
