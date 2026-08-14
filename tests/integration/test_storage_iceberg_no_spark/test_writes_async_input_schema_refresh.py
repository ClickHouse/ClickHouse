import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    drop_iceberg_table,
    get_uuid_str,
)


@pytest.mark.parametrize("format_version", [2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_async_input_refreshes_destination_schema(
    started_cluster_iceberg_no_spark, format_version, storage_type
):
    """
    Async HTTP INSERT ... SELECT FROM input() into a datalake destination must refresh
    the external schema before request-time conversion, exactly like the synchronous
    INSERT ... SELECT path (InterpreterInsertQuery::execute calls
    updateExternalDynamicMetadataIfExists before snapshotting metadata).

    Two independent nodes give independent metadata caches: node1 evolves the schema
    (RENAME), node2 keeps a stale cache until the async path refreshes it. Without the
    refresh node2 casts against the old column name and the insert fails.
    """
    writer = started_cluster_iceberg_no_spark.instances["node1"]
    reader = started_cluster_iceberg_no_spark.instances["node2"]

    # Unique name so reruns in the same (not recreated) container never collide.
    table_name = "test_async_input_schema_refresh_" + storage_type + "_" + get_uuid_str()

    # Clean any leftover definitions on both nodes before starting.
    drop_iceberg_table(writer, table_name, if_exists=True)
    drop_iceberg_table(reader, table_name, if_exists=True)

    try:
        # node1 owns the table and writes the initial schema/data.
        create_iceberg_table(
            storage_type,
            writer,
            table_name,
            started_cluster_iceberg_no_spark,
            "(id Int32, value Nullable(String))",
            format_version,
        )
        writer.query(f"INSERT INTO {table_name} VALUES (1, 'hello');")

        # node2 opens the same storage location and caches the current schema (value).
        # IF NOT EXISTS makes createInitial skip re-creation (metadata already exists) and
        # attach to the existing Iceberg table instead of failing with TABLE_ALREADY_EXISTS.
        create_iceberg_table(
            storage_type,
            reader,
            table_name,
            started_cluster_iceberg_no_spark,
            "(id Int32, value Nullable(String))",
            format_version,
            if_not_exists=True,
        )
        assert reader.query(f"SELECT id, value FROM {table_name} ORDER BY id") == "1\thello\n"

        # node1 evolves the schema. node2's cached metadata is now stale.
        writer.query(f"ALTER TABLE {table_name} RENAME COLUMN value TO label;")

        # Async HTTP INSERT ... SELECT FROM input() on the node with the stale cache.
        # The new column name only exists after the external-metadata refresh.
        query = (
            f"INSERT INTO {table_name} (id, label) "
            f"SELECT id, label FROM input('id Int32, label String') FORMAT TSV"
        )
        reader.http_query(
            query,
            data="2\tworld\n",
            params={"async_insert": "1", "wait_for_async_insert": "1"},
        )

        assert (
            reader.query(f"SELECT id, label FROM {table_name} ORDER BY id")
            == "1\thello\n2\tworld\n"
        )
    finally:
        drop_iceberg_table(writer, table_name, if_exists=True)
        drop_iceberg_table(reader, table_name, if_exists=True)
