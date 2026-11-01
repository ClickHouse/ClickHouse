"""Keeper-backed incremental reads over object storage.

Each Spark INSERT is one APPEND snapshot; snapshots become visible to
ClickHouse when the table directory is re-uploaded, so several snapshots can
arrive "at once" between metadata refreshes — unlike the local writer-sidecar
suite (test_paimon_incremental_read), which paces commits one by one.

Not covered here (not deterministically testable in integration tests):
the at-most-once delivery semantics (the Keeper watermark advances at file
collection time, before consumption) and the ephemeral processing-lock race.
"""

import pytest

from helpers.paimon_utils import (
    create_incremental_paimon_table,
    create_spark_paimon_table,
    drop_tables_at_exit,
    get_creation_expression,
    get_uuid_str,
    id_array_str,
    incremental_settings,
    spark_table,
    upload_paimon_table,
    wait_until_query_result,
)


def _insert_rows(started_cluster, storage_type, table_name, start_id, rows, commits=1):
    spark = started_cluster.spark_session
    for i in range(commits):
        spark.sql(
            f"INSERT INTO {spark_table(table_name)} "
            f"SELECT id + {start_id + i * rows}, 'x' FROM range({rows})"
        )
    upload_paimon_table(started_cluster, storage_type, table_name)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_incremental_read_basic(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    table_name = "test_inc_basic_" + storage_type + "_" + get_uuid_str()
    # {database}/{table} macro expansion in paimon_keeper_path is part of the
    # documented surface; {uuid}/{replica} are covered by
    # test_paimon_incremental_read. The expanded path is unique per test since
    # the table name carries a uuid suffix.
    instance = create_incremental_paimon_table(
        started_cluster,
        storage_type,
        table_name,
        keeper_path="/clickhouse/paimon/{database}/{table}",
    )
    with drop_tables_at_exit(instance, table_name):
        # The macros must have expanded in Keeper: the replica state lives
        # under the expanded path, and nothing was created under the
        # literal-brace path (which would make distinct tables collide).
        zk = started_cluster.get_kazoo_client("zoo1")
        try:
            expanded = f"/clickhouse/paimon/default/{table_name}"
            assert zk.exists(f"{expanded}/replicas/r1/is_active") is not None
            assert zk.exists("/clickhouse/paimon/{database}/{table}") is None
        finally:
            zk.stop()

        count_query = f"SELECT count() FROM {table_name}"

        # First read is a full base scan establishing the watermark; then drained.
        wait_until_query_result(instance, count_query, "1\n")
        wait_until_query_result(instance, count_query, "0\n")

        for start_id in (1, 11):
            _insert_rows(started_cluster, storage_type, table_name, start_id, 10)
            wait_until_query_result(instance, count_query, "10\n")
            wait_until_query_result(instance, count_query, "0\n")


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_incremental_read_target_snapshot(
    started_cluster_paimon_with_spark, storage_type
):
    started_cluster = started_cluster_paimon_with_spark
    table_name = "test_inc_target_" + storage_type + "_" + get_uuid_str()
    instance = create_incremental_paimon_table(started_cluster, storage_type, table_name)
    with drop_tables_at_exit(instance, table_name):
        count_query = f"SELECT count() FROM {table_name}"
        ids_query = f"SELECT arraySort(groupArray(id)) FROM {table_name}"

        wait_until_query_result(instance, count_query, "1\n")
        wait_until_query_result(instance, count_query, "0\n")

        # Two pending snapshots with distinct id sets: snapshot 2 (1..10) and
        # snapshot 3 (11..20). Distinct sets are what makes the targeted read
        # provable: an implementation returning "the current pending delta"
        # (1..20) or the wrong snapshot fails the id assertions below.
        _insert_rows(started_cluster, storage_type, table_name, 1, 10, commits=2)

        # Targeted reads deliver exactly the requested snapshot's delta, are
        # repeatable, and do not advance the watermark.
        target_3 = f"{ids_query} SETTINGS paimon_target_snapshot_id = 3"
        wait_until_query_result(instance, target_3, id_array_str(range(11, 21)))
        wait_until_query_result(instance, target_3, id_array_str(range(11, 21)))
        target_2 = f"{ids_query} SETTINGS paimon_target_snapshot_id = 2"
        wait_until_query_result(instance, target_2, id_array_str(range(1, 11)))

        # Both snapshots are still pending for the regular incremental read.
        wait_until_query_result(instance, ids_query, id_array_str(range(1, 21)))
        wait_until_query_result(instance, count_query, "0\n")


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_incremental_read_max_consume_snapshots(
    started_cluster_paimon_with_spark, storage_type
):
    started_cluster = started_cluster_paimon_with_spark
    table_name = "test_inc_limit_" + storage_type + "_" + get_uuid_str()
    instance = create_incremental_paimon_table(started_cluster, storage_type, table_name)
    with drop_tables_at_exit(instance, table_name):
        count_query = f"SELECT count() FROM {table_name}"

        wait_until_query_result(instance, count_query, "1\n")
        wait_until_query_result(instance, count_query, "0\n")

        # Three snapshots of 10 rows each (ids 1..10, 11..20, 21..30) arrive in
        # one upload. Assert the delivered id sets, not counts: a count cannot
        # distinguish a correct batch from a replay cancelling a drop.
        _insert_rows(started_cluster, storage_type, table_name, 1, 10, commits=3)

        limited_query = (
            f"SELECT arraySort(groupArray(id)) FROM {table_name} "
            f"SETTINGS max_consume_snapshots = 2"
        )
        wait_until_query_result(instance, limited_query, id_array_str(range(1, 21)))
        wait_until_query_result(instance, limited_query, id_array_str(range(21, 31)))
        wait_until_query_result(instance, limited_query, "[]\n")


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_incremental_read_keeper_state(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    table_name = "test_inc_keeper_" + storage_type + "_" + get_uuid_str()
    instance = create_incremental_paimon_table(started_cluster, storage_type, table_name)
    with drop_tables_at_exit(instance, table_name):
        count_query = f"SELECT count() FROM {table_name}"
        keeper_path = f"/clickhouse/paimon/{table_name}"

        zk = started_cluster.get_kazoo_client("zoo1")
        try:
            assert zk.exists(f"{keeper_path}/replicas/r1/is_active") is not None

            wait_until_query_result(instance, count_query, "1\n")
            assert zk.get(f"{keeper_path}/committed_snapshot")[0] == b"1"

            _insert_rows(started_cluster, storage_type, table_name, 1, 10)
            wait_until_query_result(instance, count_query, "10\n")
            assert zk.get(f"{keeper_path}/committed_snapshot")[0] == b"2"

            # Targeted reads must not advance the committed watermark.
            _insert_rows(started_cluster, storage_type, table_name, 11, 10)
            target_query = f"{count_query} SETTINGS paimon_target_snapshot_id = 3"
            wait_until_query_result(instance, target_query, "10\n")
            assert zk.get(f"{keeper_path}/committed_snapshot")[0] == b"2"
        finally:
            zk.stop()


def test_shared_watermark_competing_consumers(started_cluster_paimon_with_spark):
    """Customer journey: HA ingestion — two replicas configured with the same
    paimon_keeper_path (distinct replica names) are competing consumers over a
    shared watermark: every snapshot is delivered exactly once across them."""
    started_cluster = started_cluster_paimon_with_spark
    spark = started_cluster.spark_session
    node1 = started_cluster.instances["node1"]
    node2 = started_cluster.instances["node2"]
    table_name = "test_inc_shared_s3_" + get_uuid_str()

    create_spark_paimon_table(spark, table_name, "id BIGINT, val STRING")
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (0, 'warmup')")
    upload_paimon_table(started_cluster, "s3", table_name)

    with drop_tables_at_exit(node1, table_name), drop_tables_at_exit(node2, table_name):
        for node, replica in ((node1, "r1"), (node2, "r2")):
            node.query(
                get_creation_expression(
                    "s3",
                    table_name,
                    started_cluster,
                    table_function=False,
                    additional_settings=incremental_settings(
                        table_name, replica_name=replica
                    ),
                ),
                settings={"allow_experimental_paimon_storage_engine": 1},
            )
        count_query = f"SELECT count() FROM {table_name}"

        # node1 consumes the base; the shared watermark means node2 gets nothing.
        wait_until_query_result(node1, count_query, "1\n")
        wait_until_query_result(node1, count_query, "0\n")
        assert node2.query(count_query) == "0\n"

        # A new snapshot consumed by node2 must not be re-delivered to node1.
        # The exact id set, not just count(): a broken shared watermark that
        # replays the warmup row and drops a pending row would still deliver
        # 10 rows.
        _insert_rows(started_cluster, "s3", table_name, 1, 10)
        ids_query = f"SELECT arraySort(groupArray(id)) FROM {table_name}"
        wait_until_query_result(node2, ids_query, id_array_str(range(1, 11)))
        wait_until_query_result(node2, count_query, "0\n")
        assert node1.query(count_query) == "0\n"


def test_incremental_read_without_background_refresh(started_cluster_paimon_with_spark):
    """paimon_metadata_refresh_interval_sec = 0 disables the background
    refresh task; incremental reads must still consume new snapshots because
    engine metadata is also refreshed per query."""
    started_cluster = started_cluster_paimon_with_spark
    table_name = "test_inc_no_refresh_s3_" + get_uuid_str()
    instance = create_incremental_paimon_table(
        started_cluster, "s3", table_name, refresh_interval_sec=0
    )
    with drop_tables_at_exit(instance, table_name):
        count_query = f"SELECT count() FROM {table_name}"
        wait_until_query_result(instance, count_query, "1\n")
        wait_until_query_result(instance, count_query, "0\n")

        # The exact id set, not just count(): a broken per-query refresh that
        # replays the warmup row and drops a pending row would still deliver
        # 10 rows.
        _insert_rows(started_cluster, "s3", table_name, 1, 10)
        ids_query = f"SELECT arraySort(groupArray(id)) FROM {table_name}"
        wait_until_query_result(instance, ids_query, id_array_str(range(1, 11)))
        wait_until_query_result(instance, count_query, "0\n")


def test_incremental_read_requires_keeper_settings(started_cluster_paimon_with_spark):
    """paimon_incremental_read without paimon_keeper_path/paimon_replica_name
    is rejected at CREATE time. (A replica-name conflict, by contrast, is only
    a warning at CREATE and is not deterministically assertable.)"""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_inc_no_keeper_" + get_uuid_str()

    create_spark_paimon_table(spark, table_name, "id BIGINT, val STRING")
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (0, 'warmup')")
    upload_paimon_table(started_cluster, "s3", table_name)

    create_query = get_creation_expression(
        "s3",
        table_name,
        started_cluster,
        table_function=False,
        additional_settings=["paimon_incremental_read = 1"],
    )
    error = instance.query_and_get_error(
        create_query, settings={"allow_experimental_paimon_storage_engine": 1}
    )
    assert (
        "both paimon_keeper_path and paimon_replica_name must be specified" in error
    )


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_incremental_read_refreshable_mv(
    started_cluster_paimon_with_spark, storage_type
):
    """Paimon (incremental read) -> refreshable MV (APPEND) -> MergeTree."""
    started_cluster = started_cluster_paimon_with_spark
    table_name = "test_inc_mv_src_" + storage_type + "_" + get_uuid_str()
    dest_table = "test_inc_mv_dest_" + storage_type + "_" + get_uuid_str()
    mv_name = "test_inc_mv_" + storage_type + "_" + get_uuid_str()
    instance = create_incremental_paimon_table(started_cluster, storage_type, table_name)
    with drop_tables_at_exit(instance, table_name, dest_table):
        count_query = f"SELECT count() FROM {table_name}"

        # Establish the incremental baseline before wiring up the MV.
        wait_until_query_result(instance, count_query, "1\n")
        wait_until_query_result(instance, count_query, "0\n")

        instance.query(
            f"CREATE TABLE {dest_table} AS {table_name} ENGINE = MergeTree() ORDER BY tuple()"
        )
        instance.query(
            f"CREATE MATERIALIZED VIEW {mv_name} "
            f"REFRESH EVERY 5 SECOND APPEND TO {dest_table} "
            f"AS SELECT * FROM {table_name}"
        )
        try:
            # Assert the delivered id sets in the destination, not counts: a
            # count cannot see a replayed batch cancelling a dropped one.
            dest_ids_query = f"SELECT arraySort(groupArray(id)) FROM {dest_table}"
            _insert_rows(started_cluster, storage_type, table_name, 1, 10)
            wait_until_query_result(
                instance, dest_ids_query, id_array_str(range(1, 11)), retries=60
            )

            _insert_rows(started_cluster, storage_type, table_name, 11, 10)
            wait_until_query_result(
                instance, dest_ids_query, id_array_str(range(1, 21)), retries=60
            )
        finally:
            # Stop the background refresh before any DDL so it cannot block it.
            instance.query(f"SYSTEM STOP VIEW {mv_name}")
            instance.query(f"DROP VIEW IF EXISTS {mv_name} SYNC")
