import time

from helpers.export_partition_helpers import (
    make_iceberg_s3,
    setup_source_tables,
)

# Table factories and metadata assertions shared by the `EXPORT PARTITION` Iceberg test modules.


def create_iceberg_s3_table(node, iceberg_table: str, if_not_exists: bool = False,
                            s3_retry_attempts: int = 3):
    """Create (or attach to an existing) IcebergS3 table at a per-test MinIO prefix."""
    make_iceberg_s3(
        node, iceberg_table, "id Int64, year Int32",
        partition_by="year", if_not_exists=if_not_exists,
        s3_retry_attempts=s3_retry_attempts,
    )


def setup_tables(cluster, mt_table: str, iceberg_table: str, nodes: list | None = None,
                 s3_retry_attempts: int = 3, engine: str = "ReplicatedMergeTree"):
    """
    Create the source table on the given nodes, insert data on the first node, wait for
    replication, then create the Iceberg destination table on each node.

    A plain `MergeTree` source only exists on the first node (see `setup_source_tables`), but the
    destination is still created everywhere the caller asked for, so a test can read it back from
    any node. The Iceberg table is created on the first node (which initialises the S3 metadata);
    subsequent nodes attach to the same path with IF NOT EXISTS.

    `nodes` defaults to every instance the test module asked for in `CLUSTER_INSTANCES`, so it
    can never name an instance this cluster did not start.
    """
    if nodes is None:
        nodes = list(cluster.instances)

    instances = [cluster.instances[n] for n in nodes]
    primary = instances[0]

    setup_source_tables(
        instances,
        mt_table,
        "id Int64, year Int32",
        "year",
        engine,
        insert_values="(1, 2020), (2, 2020), (3, 2020), (4, 2021)",
        replica_names=nodes,
    )

    create_iceberg_s3_table(primary, iceberg_table, s3_retry_attempts=s3_retry_attempts)
    for instance in instances[1:]:
        create_iceberg_s3_table(instance, iceberg_table, if_not_exists=True,
                                s3_retry_attempts=s3_retry_attempts)


def _destination_paths_has_sync_failed_marker(node, source_table, dest_table, partition_id):
    """True when destination_file_paths contains the Keeper sync-failed marker value."""
    result = node.query(
        f"SELECT has(arrayFlatten(mapValues(destination_file_paths)), '<failed to read from zk>')"
        f" FROM system.partition_exports"
        f" WHERE source_table = '{source_table}'"
        f"   AND destination_table = '{dest_table}'"
        f"   AND partition_id = '{partition_id}'"
    ).strip()
    return result == "1"


def wait_for_destination_paths_sync_failed_marker(
    node, source_table, dest_table, partition_id, expect_marker, timeout=90, poll_interval=0.5
):
    """Wait until destination_file_paths does/does not contain the sync-failed marker.

    The in-memory mirror refreshes on the manifest-updater poll (~30s), so the
    default timeout allows at least one full cycle plus headroom.
    """
    start_time = time.time()
    last = None
    while time.time() - start_time < timeout:
        last = _destination_paths_has_sync_failed_marker(
            node, source_table, dest_table, partition_id
        )
        if last == expect_marker:
            return
        time.sleep(poll_interval)

    raise TimeoutError(
        f"destination_file_paths sync-failed marker did not become {expect_marker}"
        f" within {timeout}s (last={last})"
    )


def data_file_partition_records(entries):
    """Partition dicts of the non-delete data files described by manifest entries."""
    records = []
    for entry in entries:
        data_file = entry.get("data_file") or {}
        if data_file.get("content", 0) not in (0, None):
            continue
        partition = data_file.get("partition")
        if partition is not None:
            records.append(partition)
    return records


def partition_scalar(partition, field):
    """Read a partition field value, tolerating an Avro-union ``{type: value}`` wrapper."""
    value = partition.get(field)
    if isinstance(value, dict):
        assert len(value) == 1, f"Unexpected partition union shape for {field!r}: {value!r}"
        value = next(iter(value.values()))
    return value
