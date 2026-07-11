import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_optimize_after_rename_column(started_cluster_iceberg_no_spark, storage_type):
    """
    OPTIMIZE (compaction) must read data files written before an `ALTER ... RENAME COLUMN`
    by the file's own schema and remap the columns to the current schema. Otherwise the old
    file is read by the current-schema column names and the renamed column is compacted into
    DEFAULT/NULL values instead of the original data.

    A DELETE creates a positional delete file (merge-on-read) so compaction has work to do.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_after_rename_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        2,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'hello'), (2, 'world'), (3, 'foo');",
        settings={"allow_insert_into_iceberg": 1},
    )
    # DELETE one row -> produces a positional delete file (merge-on-read),
    # which makes compaction necessary.
    instance.query(
        f"DELETE FROM {TABLE_NAME} WHERE id = 2;",
        settings={"allow_insert_into_iceberg": 1},
    )

    # Schema evolution: the data file above now belongs to an older schema id.
    instance.query(f"ALTER TABLE {TABLE_NAME} RENAME COLUMN value TO label;")

    assert (
        instance.query(f"SELECT id, label FROM {TABLE_NAME} ORDER BY id")
        == "1\thello\n3\tfoo\n"
    )

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={"allow_experimental_iceberg_compaction": 1, "allow_insert_into_iceberg": 1},
    )

    # After compaction the renamed column must still carry the original values,
    # not DEFAULT/NULL, and the deleted row must stay deleted.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 2
    assert (
        instance.query(f"SELECT id, label FROM {TABLE_NAME} ORDER BY id")
        == "1\thello\n3\tfoo\n"
    )
    assert (
        int(instance.query(f"SELECT count() FROM {TABLE_NAME} WHERE label IS NULL"))
        == 0
    )
