import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)

# One row per data file, so a read has to open this many separate objects.
NUM_DATA_FILES = 40


def run_and_get_profile_events(instance, query, query_id, settings):
    """Run `query` and return its own ProfileEvents."""
    result = instance.query(query, query_id=query_id, settings=settings)
    instance.query("SYSTEM FLUSH LOGS")
    events = instance.query(
        f"""
        SELECT
            ProfileEvents['S3HeadObject'],
            ProfileEvents['IcebergManifestObjectMetadataUsed']
        FROM system.query_log
        WHERE query_id = '{query_id}' AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        """
    ).strip()
    head_objects, manifest_metadata_used = (int(value) for value in events.split("\t"))
    return result, head_objects, manifest_metadata_used


@pytest.mark.parametrize("storage_type", ["s3"])
def test_manifest_object_metadata_avoids_head_request_per_data_file(
    started_cluster_iceberg_no_spark, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_manifest_object_metadata_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type, instance, table_name, started_cluster_iceberg_no_spark, "(x Int32)"
    )

    instance.query(
        f"INSERT INTO {table_name} SELECT number FROM numbers({NUM_DATA_FILES})",
        settings={
            "iceberg_insert_max_rows_in_data_file": 1,
            "max_block_size": 1,
            "min_insert_block_size_rows": 1,
            "min_insert_block_size_bytes": 1,
        },
    )

    assert (
        int(instance.query(f"SELECT uniqExact(_path) FROM {table_name}")) == NUM_DATA_FILES
    )

    # Need a query that iceberg can't trivially answer from metadata
    read_query = f"SELECT sum(x) FROM {table_name}"

    # Cache warming execution
    instance.query(read_query, settings={"use_iceberg_manifest_object_metadata": 1})

    enabled_result, enabled_heads, enabled_used = run_and_get_profile_events(
        instance,
        read_query,
        query_id=f"{table_name}_enabled",
        settings={"use_iceberg_manifest_object_metadata": 1},
    )
    disabled_result, disabled_heads, disabled_used = run_and_get_profile_events(
        instance,
        read_query,
        query_id=f"{table_name}_disabled",
        settings={"use_iceberg_manifest_object_metadata": 0},
    )

    # This option should not change correctness.
    assert enabled_result == disabled_result

    # The manifest answered for every data file and for nothing else.
    assert enabled_used == NUM_DATA_FILES
    assert disabled_used == 0

    # Which matches the number of requests that did not have to be made
    assert disabled_heads - enabled_heads == NUM_DATA_FILES
