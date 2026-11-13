import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    get_uuid_str,
)


@pytest.mark.parametrize("storage_type", ["local"])
def test_trivial_count_dangling_position_deletes(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_trivial_count_dangling_position_deletes_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id bigint) USING iceberg TBLPROPERTIES ('format-version' = '2',
        'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id FROM range(5)")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id IN (1, 3)")
    # Compaction applies the deletes to the rewritten data file and leaves the delete file dangling.
    spark.sql(
        f"CALL spark_catalog.system.rewrite_data_files(table => 'default.{TABLE_NAME}', options => map('rewrite-all', 'true'))"
    )
    assert spark.sql(f"SELECT count(*) FROM {TABLE_NAME}").collect()[0][0] == 3

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    assert instance.query(f"SELECT count() FROM {TABLE_NAME}", settings={"optimize_trivial_count_query": 0}) == "3\n"
    assert instance.query(f"SELECT count() FROM {TABLE_NAME}", settings={"optimize_trivial_count_query": 1}) == "3\n"
