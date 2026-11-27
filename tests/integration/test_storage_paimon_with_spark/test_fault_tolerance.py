"""Fault-tolerance coverage for Paimon over object storage.

Deterministic failure modes only:
- processing-lock contention (the Keeper lock held by someone else),
- Keeper outage during incremental reads, and recovery after it heals,
- data files missing from object storage (partial upload / eventual listing),
- incremental watermark durability across a server restart.

Non-deterministic modes (session-expiry races, at-most-once loss between
watermark advance and consumption, mid-commit visibility) are documented in
the module docstrings of the incremental tests and are out of scope here.
"""

import threading
import time

import pytest

from helpers.network import PartitionManager
from helpers.paimon_utils import (
    create_incremental_paimon_table,
    create_spark_paimon_table,
    drop_tables_at_exit,
    get_creation_expression,
    get_uuid_str,
    id_array_str,
    spark_table,
    upload_paimon_table,
    wait_until_query_result,
)


def test_processing_lock_conflict(started_cluster_paimon_with_spark):
    """A processing lock held by another consumer must produce a clean
    REPLICA_IS_ALREADY_ACTIVE error, and reads must work again once the lock
    is released."""
    started_cluster = started_cluster_paimon_with_spark
    table_name = "test_ft_lock_s3_" + get_uuid_str()
    instance = create_incremental_paimon_table(started_cluster, "s3", table_name)
    with drop_tables_at_exit(instance, table_name):
        count_query = f"SELECT count() FROM {table_name}"
        wait_until_query_result(instance, count_query, "1\n")
        wait_until_query_result(instance, count_query, "0\n")
        lock_path = f"/clickhouse/paimon/{table_name}/processing_lock"

        zk = started_cluster.get_kazoo_client("zoo1")
        try:
            zk.create(lock_path, b"held-by-test")
            error = instance.query_and_get_error(count_query)
            assert "Another incremental read is in progress" in error
            zk.delete(lock_path)
            wait_until_query_result(instance, count_query, "0\n")
        finally:
            zk.stop()


def test_keeper_outage_and_recovery(started_cluster_paimon_with_spark):
    """With Keeper unreachable, incremental reads must fail cleanly; after the
    network heals, the replica must re-activate and consume new snapshots."""
    started_cluster = started_cluster_paimon_with_spark
    spark = started_cluster.spark_session
    table_name = "test_ft_outage_s3_" + get_uuid_str()
    instance = create_incremental_paimon_table(started_cluster, "s3", table_name)
    with drop_tables_at_exit(instance, table_name):
        count_query = f"SELECT count() FROM {table_name}"
        wait_until_query_result(instance, count_query, "1\n")
        wait_until_query_result(instance, count_query, "0\n")

        with PartitionManager() as pm:
            pm.drop_instance_zk_connections(instance)
            error = instance.query_and_get_error(count_query)
            assert any(
                keyword in error for keyword in ("Keeper", "ZooKeeper", "Coordination")
            ), f"expected a Keeper-related error, got: {error!r}"

        # After healing: new snapshot must be consumed (session re-establishment
        # and replica re-activation both work).
        spark.sql(f"INSERT INTO {spark_table(table_name)} SELECT id + 1, 'x' FROM range(10)")
        upload_paimon_table(started_cluster, "s3", table_name)
        # The exact id set, not just count(): a broken re-activation that
        # replays the warmup row and drops a pending row would still deliver
        # 10 rows.
        ids_query = f"SELECT arraySort(groupArray(id)) FROM {table_name}"
        wait_until_query_result(
            instance, ids_query, id_array_str(range(1, 11)), retries=120
        )
        wait_until_query_result(instance, count_query, "0\n")


def test_incremental_state_survives_restart(started_cluster_paimon_with_spark):
    """The committed watermark lives in Keeper: after a server restart the
    table must resume from it — delivering exactly the pending delta's ids
    (1..10), never re-reading the base row 0."""
    started_cluster = started_cluster_paimon_with_spark
    spark = started_cluster.spark_session
    table_name = "test_ft_restart_s3_" + get_uuid_str()
    instance = create_incremental_paimon_table(started_cluster, "s3", table_name)
    with drop_tables_at_exit(instance, table_name):
        count_query = f"SELECT count() FROM {table_name}"
        wait_until_query_result(instance, count_query, "1\n")
        wait_until_query_result(instance, count_query, "0\n")

        spark.sql(f"INSERT INTO {spark_table(table_name)} SELECT id + 1, 'x' FROM range(10)")
        upload_paimon_table(started_cluster, "s3", table_name)

        instance.restart_clickhouse()

        # The exact id set, not just count(): a broken resume that replays the
        # base row and drops a pending row would still deliver 10 rows.
        ids_query = f"SELECT arraySort(groupArray(id)) FROM {table_name}"
        wait_until_query_result(
            instance, ids_query, id_array_str(range(1, 11)), retries=120
        )
        wait_until_query_result(instance, count_query, "0\n")


def test_at_most_once_loss_window(started_cluster_paimon_with_spark):
    """Pins the documented at-most-once semantics: the Keeper watermark
    advances at file-collection time, before the batch is delivered, so a
    crash inside that window loses the batch. The failpoint
    (ClickHouse#113443) pauses exactly inside the window; the test observes
    the committed watermark in Keeper while the read is paused, kills the
    server, and asserts the batch was never delivered — while the rows still
    exist in the table itself. This test flips the day delivery becomes
    at-least-once. Skips until the binary carries the failpoint."""
    started_cluster = started_cluster_paimon_with_spark
    spark = started_cluster.spark_session
    table_name = "test_ft_amo_s3_" + get_uuid_str()
    instance = create_incremental_paimon_table(started_cluster, "s3", table_name)
    with drop_tables_at_exit(instance, table_name):
        count_query = f"SELECT count() FROM {table_name}"
        wait_until_query_result(instance, count_query, "1\n")
        wait_until_query_result(instance, count_query, "0\n")

        failpoint = "paimon_incremental_read_pause_after_watermark_commit"
        _, stderr = instance.query_and_get_answer_with_error(
            f"SYSTEM ENABLE FAILPOINT {failpoint}"
        )
        if stderr:
            pytest.skip(
                "the binary does not carry the Paimon failpoint yet "
                "(https://github.com/ClickHouse/ClickHouse/pull/113443)"
            )

        spark.sql(
            f"INSERT INTO {spark_table(table_name)} SELECT id + 1, 'x' FROM range(10)"
        )
        upload_paimon_table(started_cluster, "s3", table_name)

        # The read commits the watermark, then pauses inside the window.
        reader_result = {}
        reader = threading.Thread(
            target=lambda: reader_result.update(
                zip(("out", "err"), instance.query_and_get_answer_with_error(count_query))
            )
        )
        reader.start()
        try:
            zk = started_cluster.get_kazoo_client("zoo1")
            try:
                keeper_path = f"/clickhouse/paimon/{table_name}"
                deadline = time.monotonic() + 60
                while zk.get(f"{keeper_path}/committed_snapshot")[0] != b"2":
                    assert time.monotonic() < deadline, "watermark never advanced"
                    time.sleep(0.5)
            finally:
                zk.stop()

            # The reader must still be blocked at the failpoint: if it already
            # returned, the pause did not happen and this test proves nothing.
            assert reader.is_alive(), (
                f"the reader returned before the kill — the failpoint did not "
                f"pause inside the window: {reader_result!r}"
            )

            # Crash inside the window: watermark committed, batch not delivered.
            instance.restart_clickhouse(kill=True)
        finally:
            # On any failure before the kill, the package-scoped server is
            # still running with the failpoint armed and the reader paused at
            # it: disarm so neither leaks into later tests (a no-op on the
            # freshly restarted server after the kill).
            instance.query_and_get_answer_with_error(
                f"SYSTEM DISABLE FAILPOINT {failpoint}"
            )
            reader.join(timeout=60)
            assert not reader.is_alive(), "the reader thread never finished"
        # The killed reader must never have delivered the 10-row batch.
        assert reader_result.get("out") != "10\n", (
            f"the batch was delivered despite the kill: {reader_result!r}"
        )

        # The killed server never closed its Keeper session, so its ephemeral
        # processing lock lingers until the session expires; wait for it to go
        # away so the drain below does not hit a lock-conflict error.
        zk = started_cluster.get_kazoo_client("zoo1")
        try:
            deadline = time.monotonic() + 60
            while zk.exists(f"{keeper_path}/processing_lock") is not None:
                assert (
                    time.monotonic() < deadline
                ), "stale processing lock never expired"
                time.sleep(0.5)
        finally:
            zk.stop()

        # The batch is lost (at-most-once): the stream has nothing pending,
        # although the rows themselves are still in the table.
        wait_until_query_result(instance, count_query, "0\n", retries=120)
        table_expr = get_creation_expression(
            "s3", table_name, started_cluster, table_function=True
        )
        assert instance.query(f"SELECT count() FROM {table_expr}") == "11\n"


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_missing_data_files(started_cluster_paimon_with_spark, storage_type):
    """Metadata referencing data files absent from object storage (partial
    upload, out-of-order replication) must produce a clean error — and reads
    must succeed once the files appear."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_ft_missing_" + storage_type + "_" + get_uuid_str()

    create_spark_paimon_table(spark, table_name, "a INT NOT NULL, b STRING")
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (1, 'one'), (2, 'two')")
    upload_paimon_table(started_cluster, storage_type, table_name)

    remote_dir = f"var/lib/clickhouse/user_files/paimon_data/default.db/{table_name}/"
    if storage_type == "s3":
        client = started_cluster.minio_client
        data_keys = [
            obj.object_name
            for obj in client.list_objects(
                started_cluster.minio_bucket, prefix=remote_dir, recursive=True
            )
            if obj.object_name.endswith(".parquet")
        ]
        assert data_keys
        for key in data_keys:
            client.remove_object(started_cluster.minio_bucket, key)
    else:
        container = started_cluster.container_client
        data_blobs = [
            blob.name
            for blob in container.list_blobs(name_starts_with="/" + remote_dir)
            if blob.name.endswith(".parquet")
        ]
        assert data_blobs
        for name in data_blobs:
            container.delete_blob(name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    error = instance.query_and_get_error(f"SELECT * FROM {table_expr} ORDER BY a")
    assert error != ""
    assert instance.query("SELECT 1") == "1\n"

    # Once the files are present again, reads succeed.
    upload_paimon_table(started_cluster, storage_type, table_name)
    assert (
        instance.query(f"SELECT a, b FROM {table_expr} ORDER BY a")
        == "1\tone\n2\ttwo\n"
    )
