# coding: utf-8

import os
import threading
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

DOCKER_COMPOSE_PATH = get_docker_compose_path()

INCREMENTAL_WRITER_JAR = "/opt/paimon/paimon-incremental-writer.jar"
CLICKHOUSE_WORKDIR = "/var/lib/clickhouse"
USER_FILES_PATH = f"{CLICKHOUSE_WORKDIR}/user_files"

CH_TABLE_NAME = "paimon_inc_read"
CH_TABLE_NAME_WITH_LIMIT = "paimon_inc_read_with_limit"
CH_TABLE_NAME_AT_MOST_ONCE = "paimon_inc_read_at_most_once"
CH_TABLE_NAME_LOST_LOCK = "paimon_inc_read_lost_lock"
CH_TABLE_NAME_MONOTONIC = "paimon_inc_read_monotonic"
CH_TABLE_NAME_CURSOR_AHEAD = "paimon_inc_read_cursor_ahead"
CH_TABLE_NAME_TRANSIENT_ERROR = "paimon_inc_read_transient_error"
CH_TABLE_NAME_CONCURRENT = "paimon_inc_read_concurrent"
CH_TABLE_NAME_EXPIRED = "paimon_inc_read_expired"
CH_TABLE_NAME_HOLE = "paimon_inc_read_hole"
CH_TABLE_NAME_EXPIRED_PREFIX = "paimon_inc_read_expired_prefix"
CH_MV_PAIMON_TABLE = "paimon_mv_source"
CH_MV_MERGETREE_TABLE = "paimon_mv_dest"
CH_MV_NAME = "paimon_refresh_mv"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    stay_alive=True,
    with_zookeeper=True,
    main_configs=["configs/zookeeper.xml", "configs/config.xml"],
    macros={"shard": "s1", "replica": "r1"},
)

cluster.base_cmd.extend(
    ["--file", os.path.join(DOCKER_COMPOSE_PATH, "docker_compose_paimon_incremental_writer.yml")]
)


def wait_for_container(cluster, service_name, timeout=60):
    docker_id = cluster.get_instance_docker_id(service_name)
    container = cluster.get_docker_handle(docker_id)
    start = time.time()
    while time.time() - start < timeout:
        info = container.client.api.inspect_container(container.name)
        if info["State"]["Running"]:
            return
        time.sleep(1)
    raise Exception(f"Container {service_name} did not start in {timeout}s")


@pytest.fixture(scope="module")
def started_cluster():
    cluster.start()
    try:
        wait_for_container(cluster, "paimon-incremental-writer")
        yield cluster
    finally:
        cluster.shutdown()


def _wait_until_query_result(
    query: str,
    expected: str,
    *,
    database: str,
    retries: int = 30,
    sleep_seconds: float = 0.5,
):
    last_result = ""
    for _ in range(retries):
        last_result = node.query(query, database=database)
        if last_result == expected:
            return
        time.sleep(sleep_seconds)

    raise AssertionError(
        f"Unexpected result for query: {query}\nExpected: {expected!r}\nActual: {last_result!r}"
    )


def _run_writer(
    container_id: str,
    *,
    warehouse_uri: str,
    start_id: int,
    rows_per_commit: int,
    commit_times: int,
) -> None:
    writer_cmd = (
        f"java -jar {INCREMENTAL_WRITER_JAR} "
        f'"{warehouse_uri}" "test" "test_table" "{start_id}" "{rows_per_commit}" "{commit_times}"'
    )
    run_and_check(
        [f"docker exec {container_id} bash -c '{writer_cmd}'"],
        shell=True,
    )


def _create_clickhouse_table_for_paimon_incremental_read(
    table_name: str,
    table_path: str,
    refresh_interval_sec: int = 1,
    keeper_path: str = "/clickhouse/tables/{uuid}",
):
    node.query(f"DROP TABLE IF EXISTS {table_name} SYNC;")
    node.query(
        "CREATE TABLE {table_name} "
        "ENGINE = PaimonLocal('{table_path}') "
        "SETTINGS "
        "paimon_incremental_read = 1, "
        "paimon_keeper_path = '{keeper_path}', "
        "paimon_replica_name = '{{replica}}', "
        "paimon_metadata_refresh_interval_sec = {refresh_interval_sec}".format(
            table_name=table_name,
            table_path=table_path,
            keeper_path=keeper_path,
            refresh_interval_sec=refresh_interval_sec,
        ),
        settings={"allow_experimental_paimon_storage_engine": 1},
    )


def _clean_warehouse(container_id: str, warehouse_dir: str):
    run_and_check(
        [f'docker exec {container_id} bash -c "rm -rf {warehouse_dir}"'],
        shell=True,
    )


def _warehouse_shell(container_id: str, command: str):
    run_and_check([f"docker exec {container_id} bash -c '{command}'"], shell=True)


def _wait_for_znode(zk, path: str, *, present: bool = True, timeout: float = 60):
    deadline = time.monotonic() + timeout
    while (zk.exists(path) is not None) != present:
        assert time.monotonic() < deadline, (
            f"znode {path} was still {'absent' if present else 'present'} after {timeout}s"
        )
        time.sleep(0.2)


def _drain_baseline(count_query: str, warm_up_rows: str):
    """Consume the warm-up snapshot so the stream starts from a known watermark."""
    _wait_until_query_result(count_query, warm_up_rows, database="default")
    _wait_until_query_result(count_query, "0\n", database="default")


def test_paimon_incremental_read_via_paimon_table_engine(started_cluster):
    writer_container_id = cluster.get_instance_docker_id("paimon-incremental-writer")

    warehouse_name = "warehouse_inc"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{USER_FILES_PATH}/{warehouse_name}/test.db/test_table"

    _clean_warehouse(writer_container_id, warehouse_dir)

    # Warm-up commit: ensure there is at least one parquet file so schema can be inferred.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)

    _create_clickhouse_table_for_paimon_incremental_read(CH_TABLE_NAME, table_path)

    # Consume warm-up snapshot and reset incremental state baseline.
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "1\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "0\n",
        database="default",
    )

    # First snapshot: 10 rows.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=1)
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "10\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "0\n",
        database="default",
    )

    # Second snapshot: another 10 rows.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=11, rows_per_commit=10, commit_times=1)
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "10\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "0\n",
        database="default",
    )

    # Targeted snapshot reads are deterministic and do not advance stream state.
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME} SETTINGS paimon_target_snapshot_id=2",
        "10\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME} SETTINGS paimon_target_snapshot_id=2",
        "10\n",
        database="default",
    )

    # max_consume_snapshots limit: consume at most 2 snapshots per query.
    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME} SYNC;")
    _clean_warehouse(writer_container_id, warehouse_dir)

    # Recreate clean Paimon table with one warm-up snapshot for schema inference.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)
    _create_clickhouse_table_for_paimon_incremental_read(CH_TABLE_NAME_WITH_LIMIT, table_path)

    # Consume warm-up snapshot before testing max_consume_snapshots behavior.
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME_WITH_LIMIT}",
        "1\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME_WITH_LIMIT}",
        "0\n",
        database="default",
    )

    # Produce 3 snapshots, each snapshot contains 10 rows.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=3)
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME_WITH_LIMIT} SETTINGS max_consume_snapshots=2",
        "20\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME_WITH_LIMIT} SETTINGS max_consume_snapshots=2",
        "10\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME_WITH_LIMIT} SETTINGS max_consume_snapshots=2",
        "0\n",
        database="default",
    )

    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME} SYNC;")
    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME_WITH_LIMIT} SYNC;")


def test_paimon_incremental_read_at_most_once_on_crash(started_cluster):
    """Pins the at-most-once delivery semantics: the Keeper watermark advances
    at file-collection time, before the batch is delivered, so a crash inside
    that window loses the batch. The `paimon_incremental_read_pause_after_watermark_commit`
    failpoint pauses exactly inside the window; the test observes the committed
    watermark advance in Keeper while the read is paused, kills the server, and
    asserts the batch was never delivered — while the rows still exist in the
    table itself. This test flips the day delivery becomes at-least-once."""
    writer_container_id = cluster.get_instance_docker_id("paimon-incremental-writer")

    warehouse_name = "warehouse_amo"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{USER_FILES_PATH}/{warehouse_name}/test.db/test_table"
    # Unique per run: committed_snapshot persists in Keeper, so a rerun
    # against the same cluster must not inherit a failed run's watermark.
    # A test-local value stays constant across the in-test server restart.
    keeper_path = f"/clickhouse/paimon_at_most_once_{uuid.uuid4().hex}"

    _clean_warehouse(writer_container_id, warehouse_dir)

    # Warm-up commit (snapshot 1), consumed to establish the baseline.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)
    _create_clickhouse_table_for_paimon_incremental_read(
        CH_TABLE_NAME_AT_MOST_ONCE, table_path, keeper_path=keeper_path
    )
    count_query = f"SELECT count() FROM {CH_TABLE_NAME_AT_MOST_ONCE}"
    _wait_until_query_result(count_query, "1\n", database="default")
    _wait_until_query_result(count_query, "0\n", database="default")

    node.query(
        "SYSTEM ENABLE FAILPOINT paimon_incremental_read_pause_after_watermark_commit"
    )

    # Snapshot 2: the batch that will be lost.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=1)

    # The read commits the watermark, then pauses inside the window.
    reader_result = {}
    reader = threading.Thread(
        target=lambda: reader_result.update(
            zip(("out", "err"), node.query_and_get_answer_with_error(count_query))
        )
    )
    reader.start()
    restarted = False
    try:
        zk = cluster.get_kazoo_client("zoo1")
        try:
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
        node.restart_clickhouse(kill=True)
        restarted = True
    finally:
        # The failpoint is process-global and PAUSEABLE: if we failed before
        # the kill, the server is still running with it armed and the reader
        # is still blocked on it — disarm it so neither this reader thread
        # nor the next test hangs. After a kill-restart it is gone anyway.
        if not restarted:
            node.query(
                "SYSTEM DISABLE FAILPOINT paimon_incremental_read_pause_after_watermark_commit"
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
    zk = cluster.get_kazoo_client("zoo1")
    try:
        deadline = time.monotonic() + 60
        while zk.exists(f"{keeper_path}/processing_lock") is not None:
            assert time.monotonic() < deadline, "stale processing lock never expired"
            time.sleep(0.5)
    finally:
        zk.stop()

    # The batch is lost (at-most-once): the stream has nothing pending,
    # although the rows themselves are still in the table.
    _wait_until_query_result(count_query, "0\n", database="default", retries=120)
    assert node.query(f"SELECT count() FROM paimonLocal('{table_path}')") == "11\n"

    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME_AT_MOST_ONCE} SYNC;")


def test_paimon_incremental_read_lost_lock_fails_loudly(started_cluster):
    """A read that no longer holds the processing lock must not commit its watermark.

    The `paimon_incremental_read_pause_before_watermark_commit` failpoint parks the read
    after it collected the batch but before it advances the watermark, and the lock is
    replaced by a node this read does not own in that window. It must then fail rather
    than overwrite the new holder's progress, and must leave the new holder's lock alone.

    The competitor here is created straight in Keeper rather than through
    `acquireProcessingLock`, so what rejects the commit is the version check: a bare
    `create` leaves the node at version 0, while an acquisition leaves it at 1. Losing
    the lock for real is a session event - Keeper drops the ephemeral only when its
    session expires, and the commit runs through that same session - and is rejected by
    `check_session_valid` instead, which this test does not exercise."""
    writer_container_id = cluster.get_instance_docker_id("paimon-incremental-writer")

    warehouse_name = "warehouse_lost_lock"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{warehouse_dir}/test.db/test_table"
    keeper_path = f"/clickhouse/paimon_lost_lock_{uuid.uuid4().hex}"

    _clean_warehouse(writer_container_id, warehouse_dir)
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)
    _create_clickhouse_table_for_paimon_incremental_read(
        CH_TABLE_NAME_LOST_LOCK, table_path, keeper_path=keeper_path
    )
    count_query = f"SELECT count() FROM {CH_TABLE_NAME_LOST_LOCK}"
    _drain_baseline(count_query, "1\n")

    node.query(
        "SYSTEM ENABLE FAILPOINT paimon_incremental_read_pause_before_watermark_commit"
    )
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=1)

    reader_result = {}
    reader = threading.Thread(
        target=lambda: reader_result.update(
            zip(("out", "err"), node.query_and_get_answer_with_error(count_query))
        )
    )
    reader.start()

    # The competitor's lock is ephemeral, so this session must outlive the reader.
    zk = cluster.get_kazoo_client("zoo1")
    try:
        lock_path = f"{keeper_path}/processing_lock"
        _wait_for_znode(zk, lock_path, present=True)
        assert reader.is_alive(), (
            f"the reader returned before the lock was taken away: {reader_result!r}"
        )

        # The lock node is gone and something else now sits at that path. Not a session
        # expiry: this reader's session is still alive, which is what makes the version
        # check rather than the session check the thing that has to catch it.
        zk.delete(lock_path)
        zk.create(lock_path, b"competitor", ephemeral=True)

        node.query(
            "SYSTEM DISABLE FAILPOINT paimon_incremental_read_pause_before_watermark_commit"
        )
        reader.join(timeout=120)
        assert not reader.is_alive(), "the reader thread never finished"

        assert "INVALID_STATE" in reader_result.get("err", ""), (
            f"the read committed without holding the lock: {reader_result!r}"
        )
        assert zk.get(f"{keeper_path}/committed_snapshot")[0] == b"1", (
            "the watermark was advanced by a read that had lost the lock"
        )
        assert zk.exists(lock_path) is not None, (
            "the read released a processing lock owned by another consumer"
        )
        assert zk.get(lock_path)[0] == b"competitor"
    finally:
        node.query(
            "SYSTEM DISABLE FAILPOINT paimon_incremental_read_pause_before_watermark_commit"
        )
        zk.stop()
        reader.join(timeout=60)

    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME_LOST_LOCK} SYNC;")


def test_paimon_incremental_read_watermark_is_monotonic(started_cluster):
    """The watermark must never move backwards.

    A read is parked before its commit while the watermark is advanced underneath
    it, so its own commit would move the cursor back and re-deliver snapshots that
    another consumer already acknowledged. It must fail instead."""
    writer_container_id = cluster.get_instance_docker_id("paimon-incremental-writer")

    warehouse_name = "warehouse_monotonic"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{warehouse_dir}/test.db/test_table"
    keeper_path = f"/clickhouse/paimon_monotonic_{uuid.uuid4().hex}"

    _clean_warehouse(writer_container_id, warehouse_dir)
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)
    _create_clickhouse_table_for_paimon_incremental_read(
        CH_TABLE_NAME_MONOTONIC, table_path, keeper_path=keeper_path
    )
    count_query = f"SELECT count() FROM {CH_TABLE_NAME_MONOTONIC}"
    _drain_baseline(count_query, "1\n")

    node.query(
        "SYSTEM ENABLE FAILPOINT paimon_incremental_read_pause_before_watermark_commit"
    )
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=1)

    reader_result = {}
    reader = threading.Thread(
        target=lambda: reader_result.update(
            zip(("out", "err"), node.query_and_get_answer_with_error(count_query))
        )
    )
    reader.start()

    zk = cluster.get_kazoo_client("zoo1")
    try:
        _wait_for_znode(zk, f"{keeper_path}/processing_lock", present=True)
        assert reader.is_alive(), (
            f"the reader returned before the watermark was moved: {reader_result!r}"
        )
        # Another consumer got to snapshot 2 first.
        zk.set(f"{keeper_path}/committed_snapshot", b"2")

        node.query(
            "SYSTEM DISABLE FAILPOINT paimon_incremental_read_pause_before_watermark_commit"
        )
        reader.join(timeout=120)
        assert not reader.is_alive(), "the reader thread never finished"

        assert "INVALID_STATE" in reader_result.get("err", ""), (
            f"the watermark was allowed to move backwards: {reader_result!r}"
        )
        assert zk.get(f"{keeper_path}/committed_snapshot")[0] == b"2"
    finally:
        node.query(
            "SYSTEM DISABLE FAILPOINT paimon_incremental_read_pause_before_watermark_commit"
        )
        zk.stop()
        reader.join(timeout=60)

    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME_MONOTONIC} SYNC;")


def test_paimon_incremental_read_cursor_ahead_of_warehouse(started_cluster):
    """A cursor left ahead of the warehouse must fail, not report "no new data".

    The warehouse is rewound below the committed watermark. Reporting an empty
    result here (which a plain `committed >= latest` comparison does) silently and
    permanently drops every later commit at an id below the stale watermark."""
    writer_container_id = cluster.get_instance_docker_id("paimon-incremental-writer")

    warehouse_name = "warehouse_cursor_ahead"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{warehouse_dir}/test.db/test_table"
    keeper_path = f"/clickhouse/paimon_cursor_ahead_{uuid.uuid4().hex}"

    _clean_warehouse(writer_container_id, warehouse_dir)
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)
    _create_clickhouse_table_for_paimon_incremental_read(
        CH_TABLE_NAME_CURSOR_AHEAD, table_path, keeper_path=keeper_path
    )
    count_query = f"SELECT count() FROM {CH_TABLE_NAME_CURSOR_AHEAD}"
    _drain_baseline(count_query, "1\n")

    # Snapshots 2 and 3, both consumed: the watermark reaches 3.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=2)
    _wait_until_query_result(count_query, "20\n", database="default")

    zk = cluster.get_kazoo_client("zoo1")
    try:
        assert zk.get(f"{keeper_path}/committed_snapshot")[0] == b"3"

        # Rewind the warehouse to snapshot 1, as restoring an older backup would.
        # The LATEST hint is deliberately left pointing at 3: getLatestTableSnapshotInfo
        # must notice it is stale and fall back to listing the snapshot directory.
        _warehouse_shell(
            writer_container_id,
            f"rm -f {table_path}/snapshot/snapshot-2 {table_path}/snapshot/snapshot-3",
        )

        deadline = time.monotonic() + 60
        error = ""
        while time.monotonic() < deadline:
            error = node.query_and_get_answer_with_error(count_query)[1]
            if error:
                break
            time.sleep(0.5)

        assert "INVALID_STATE" in error, (
            f"a cursor ahead of the warehouse was silently reported as no new data: {error!r}"
        )
        assert "clickhouse-keeper-client" in error, (
            f"the error does not tell the operator how to recover: {error!r}"
        )
        assert zk.get(f"{keeper_path}/committed_snapshot")[0] == b"3", (
            "the failing read modified the cursor"
        )

        # The documented recovery: point the cursor at the warehouse's current head.
        zk.set(f"{keeper_path}/committed_snapshot", b"1")
        _wait_until_query_result(count_query, "0\n", database="default")
    finally:
        zk.stop()

    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME_CURSOR_AHEAD} SYNC;")


def test_paimon_incremental_read_transient_error_does_not_burn_snapshot(started_cluster):
    """A snapshot that fails to load must not be mistaken for an expired one.

    Only snapshots below the warehouse's earliest id are legitimately gone. Any
    other failure is real: advancing the watermark past it would drop a committed
    snapshot permanently, so the read must fail and stay retryable."""
    writer_container_id = cluster.get_instance_docker_id("paimon-incremental-writer")

    warehouse_name = "warehouse_transient"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{warehouse_dir}/test.db/test_table"
    keeper_path = f"/clickhouse/paimon_transient_{uuid.uuid4().hex}"

    _clean_warehouse(writer_container_id, warehouse_dir)
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)
    _create_clickhouse_table_for_paimon_incremental_read(
        CH_TABLE_NAME_TRANSIENT_ERROR, table_path, keeper_path=keeper_path
    )
    count_query = f"SELECT count() FROM {CH_TABLE_NAME_TRANSIENT_ERROR}"
    _drain_baseline(count_query, "1\n")

    # Snapshots 2 and 3 are pending. Snapshot 2 is corrupted while 3 stays intact, so the
    # failure happens inside the incremental scan rather than while resolving the latest
    # snapshot. Snapshot 1 is still present, so 2 is above the warehouse's earliest id and
    # cannot have been expired.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=2)
    snapshot_file = f"{table_path}/snapshot/snapshot-2"
    _warehouse_shell(writer_container_id, f"cp {snapshot_file} {snapshot_file}.bak")
    _warehouse_shell(writer_container_id, f"echo not-json > {snapshot_file}")

    zk = cluster.get_kazoo_client("zoo1")
    try:
        deadline = time.monotonic() + 60
        error = ""
        while time.monotonic() < deadline:
            error = node.query_and_get_answer_with_error(count_query)[1]
            if error:
                break
            time.sleep(0.5)

        assert error, "an unreadable snapshot was silently skipped"
        assert zk.get(f"{keeper_path}/committed_snapshot")[0] == b"1", (
            "the watermark advanced past a snapshot that could not be read"
        )
        # The raw backend error alone does not say which table stalled or how to move on.
        assert "snapshot 2" in error, (
            f"the error does not name the snapshot that could not be read: {error!r}"
        )
        assert "clickhouse-keeper-client" in error, (
            f"the error does not tell the operator how to abandon the snapshot: {error!r}"
        )

        # The snapshot becomes readable again: nothing was lost.
        _warehouse_shell(writer_container_id, f"mv {snapshot_file}.bak {snapshot_file}")
        _wait_until_query_result(count_query, "20\n", database="default", retries=120)
        assert zk.get(f"{keeper_path}/committed_snapshot")[0] == b"3"
    finally:
        zk.stop()

    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME_TRANSIENT_ERROR} SYNC;")


def test_paimon_incremental_read_concurrent_reader_is_not_a_rewind(started_cluster):
    """Another consumer getting ahead of us is not a rewound warehouse.

    A read pins its snapshot state during analysis but reads the watermark at
    execution time, under the processing lock. A second consumer sharing the cursor
    can complete a whole poll in that gap and leave the watermark above the pinned
    state without anything being rolled back. Reporting that as a rewind would be a
    false alarm, and the recovery command in that error would rewind the cursor to
    the stale pinned id and re-deliver snapshots the other consumer already sent."""
    writer_container_id = cluster.get_instance_docker_id("paimon-incremental-writer")

    warehouse_name = "warehouse_concurrent"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{warehouse_dir}/test.db/test_table"
    keeper_path = f"/clickhouse/paimon_concurrent_{uuid.uuid4().hex}"

    _clean_warehouse(writer_container_id, warehouse_dir)
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)
    _create_clickhouse_table_for_paimon_incremental_read(
        CH_TABLE_NAME_CONCURRENT, table_path, keeper_path=keeper_path
    )
    count_query = f"SELECT count() FROM {CH_TABLE_NAME_CONCURRENT}"
    _drain_baseline(count_query, "1\n")

    # Snapshot 2 exists, so the parked reader below pins snapshot 2.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=1)

    # The failpoint pauses once, so only this reader parks - the competing poll below
    # runs straight through.
    node.query(
        "SYSTEM ENABLE FAILPOINT paimon_incremental_read_pause_before_processing_lock"
    )
    parked_result = {}
    parked = threading.Thread(
        target=lambda: parked_result.update(
            zip(("out", "err"), node.query_and_get_answer_with_error(count_query))
        )
    )
    parked.start()

    zk = cluster.get_kazoo_client("zoo1")
    try:
        # Wait until the reader has actually reached the failpoint rather than guessing.
        # The warehouse name makes this match specific to this test, so it cannot be
        # satisfied by a line another test left in the shared server log.
        node.wait_for_log_line(
            f"Paimon incremental read of '.*{warehouse_name}.*' pinned snapshot_id=2",
            timeout=60,
        )
        assert parked.is_alive(), (
            f"the reader returned before the competing poll ran: {parked_result!r}"
        )

        # Snapshot 3 lands and another consumer drains 2..3 while the first reader is
        # parked with snapshot 2 pinned.
        _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=11, rows_per_commit=10, commit_times=1)
        zk.set(f"{keeper_path}/committed_snapshot", b"3")

        node.query(
            "SYSTEM DISABLE FAILPOINT paimon_incremental_read_pause_before_processing_lock"
        )
        parked.join(timeout=120)
        assert not parked.is_alive(), "the reader thread never finished"

        # Watermark 3 > pinned snapshot 2, but the warehouse head is also 3: nothing was
        # rewound, so this is simply no new data.
        assert not parked_result.get("err"), (
            f"a concurrent reader's progress was reported as a rewound warehouse: {parked_result!r}"
        )
        assert parked_result.get("out") == "0\n", (
            f"expected no new data, got {parked_result!r}"
        )
        assert zk.get(f"{keeper_path}/committed_snapshot")[0] == b"3", (
            "the cursor was moved by a read that had nothing to do"
        )
    finally:
        node.query(
            "SYSTEM DISABLE FAILPOINT paimon_incremental_read_pause_before_processing_lock"
        )
        zk.stop()
        parked.join(timeout=60)

    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME_CONCURRENT} SYNC;")


def test_paimon_incremental_read_expired_snapshot_is_skipped(started_cluster):
    """Snapshots Paimon expired must still be skipped, under every EARLIEST hint state.

    This is the other half of the "unreadable snapshot fails the read" rule: only ids
    below the warehouse's earliest snapshot may be skipped, so if resolving `earliest`
    stops working, a table that merely expired old snapshots would stall forever. The
    three phases below re-run the same scan with the hint absent, valid, and stale."""
    writer_container_id = cluster.get_instance_docker_id("paimon-incremental-writer")

    warehouse_name = "warehouse_expired"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{warehouse_dir}/test.db/test_table"
    snapshot_dir = f"{table_path}/snapshot"
    keeper_path = f"/clickhouse/paimon_expired_{uuid.uuid4().hex}"

    _clean_warehouse(writer_container_id, warehouse_dir)
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)
    _create_clickhouse_table_for_paimon_incremental_read(
        CH_TABLE_NAME_EXPIRED, table_path, keeper_path=keeper_path
    )
    count_query = f"SELECT count() FROM {CH_TABLE_NAME_EXPIRED}"
    _drain_baseline(count_query, "1\n")

    # Snapshots 2, 3 and 4, then delete ids 1..2 the way Paimon expiration does - it always
    # expires starting at the earliest id, so what it removes is a prefix. Snapshot 2 is now
    # below the earliest surviving id (3) and must be skipped; 3 and 4 must be delivered.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=3)
    _warehouse_shell(
        writer_container_id, f"rm -f {snapshot_dir}/snapshot-1 {snapshot_dir}/snapshot-2"
    )

    zk = cluster.get_kazoo_client("zoo1")
    try:
        # Each phase rewinds the cursor and re-runs the identical scan, so any difference in
        # outcome is attributable to how EARLIEST was resolved.
        for phase, hint_setup in (
            # No hint at all: earliest must come from listing the snapshot directory.
            ("hint absent", f"rm -f {snapshot_dir}/EARLIEST"),
            # A hint that agrees with the directory: the fast path.
            # printf, not echo: Paimon hint files hold the bare number, and the parser
            # rejects trailing bytes - the same way it already does for LATEST.
            ("hint valid", f"printf 3 > {snapshot_dir}/EARLIEST"),
            # A hint left behind pointing at an expired snapshot. Trusting it would put
            # earliest back at 1, which would make snapshot 2 look like a real error.
            ("hint stale", f"printf 1 > {snapshot_dir}/EARLIEST"),
        ):
            _warehouse_shell(writer_container_id, hint_setup)
            zk.set(f"{keeper_path}/committed_snapshot", b"1")

            out, error = node.query_and_get_answer_with_error(count_query)
            assert not error, f"[{phase}] an expired snapshot was treated as an error: {error!r}"
            assert out == "20\n", f"[{phase}] expected snapshots 3 and 4, got {out!r}"
            assert zk.get(f"{keeper_path}/committed_snapshot")[0] == b"4", (
                f"[{phase}] the watermark did not advance past the expired snapshot"
            )
    finally:
        zk.stop()

    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME_EXPIRED} SYNC;")


def test_paimon_incremental_read_expired_prefix_is_skipped_in_one_step(started_cluster):
    """An expired prefix costs one resolution, not one per missing id.

    This is the shape the skip path exists for: a consumer that fell behind while Paimon
    expired everything it had not consumed yet. Resolving `earliest` per missing id would
    make the poll quadratic - a cursor at 1 with only ids 90000..100000 surviving would
    issue a failed read plus a directory listing ~90000 times, and `max_consume_snapshots`
    does not bound it because skipped ids never count towards the limit. Nothing below
    `earliest` exists, so the whole prefix must be settled at once.

    The prefix here is short enough to run quickly; what makes the test meaningful is the
    log assertion, since walking the prefix would emit one line per missing id."""
    writer_container_id = cluster.get_instance_docker_id("paimon-incremental-writer")

    warehouse_name = "warehouse_expired_prefix"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{warehouse_dir}/test.db/test_table"
    snapshot_dir = f"{table_path}/snapshot"
    keeper_path = f"/clickhouse/paimon_expired_prefix_{uuid.uuid4().hex}"

    _clean_warehouse(writer_container_id, warehouse_dir)
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)
    _create_clickhouse_table_for_paimon_incremental_read(
        CH_TABLE_NAME_EXPIRED_PREFIX, table_path, keeper_path=keeper_path
    )
    count_query = f"SELECT count() FROM {CH_TABLE_NAME_EXPIRED_PREFIX}"
    # Leaves the cursor at snapshot 1.
    _drain_baseline(count_query, "1\n")

    # Snapshots 2..6, then expire the prefix 1..5 the way Paimon does. The cursor is at 1,
    # so ids 2, 3, 4 and 5 are all missing and only snapshot 6 survives to be delivered.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=5)
    _warehouse_shell(
        writer_container_id,
        f"rm -f {snapshot_dir}/snapshot-1 {snapshot_dir}/snapshot-2 {snapshot_dir}/snapshot-3 "
        f"{snapshot_dir}/snapshot-4 {snapshot_dir}/snapshot-5",
    )
    _warehouse_shell(writer_container_id, f"printf 6 > {snapshot_dir}/EARLIEST")

    skip_marker = "were expired by Paimon"
    skips_before = int(node.count_in_log(skip_marker))

    zk = cluster.get_kazoo_client("zoo1")
    try:
        out, error = node.query_and_get_answer_with_error(count_query)
        assert not error, f"the expired prefix was treated as an error: {error!r}"
        assert out == "10\n", f"expected only snapshot 6, got {out!r}"
        assert zk.get(f"{keeper_path}/committed_snapshot")[0] == b"6", (
            "the watermark did not advance past the expired prefix"
        )
    finally:
        zk.stop()

    # One line for the whole prefix. Walking it id by id would log four.
    skips_after = int(node.count_in_log(skip_marker))
    assert skips_after - skips_before == 1, (
        f"expected the prefix to be settled in one step, got {skips_after - skips_before} skips"
    )
    assert node.grep_in_log("Snapshot ids 2..5 were expired by Paimon"), (
        "the skip did not cover the whole expired prefix"
    )

    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME_EXPIRED_PREFIX} SYNC;")


def test_paimon_incremental_read_missing_snapshot_is_not_expiration(started_cluster):
    """A hole in the middle of the snapshot range is not expiration.

    Paimon only deletes snapshots from the ends of the id range, so the surviving ids are
    always contiguous. A snapshot that is missing while older ones are still present was
    therefore not expired - something else lost it. Treating every missing file as expired
    (which is what assuming "removed by compaction" amounts to) would drop it silently."""
    writer_container_id = cluster.get_instance_docker_id("paimon-incremental-writer")

    warehouse_name = "warehouse_hole"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{warehouse_dir}/test.db/test_table"
    snapshot_file = f"{table_path}/snapshot/snapshot-3"
    keeper_path = f"/clickhouse/paimon_hole_{uuid.uuid4().hex}"

    _clean_warehouse(writer_container_id, warehouse_dir)
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)
    _create_clickhouse_table_for_paimon_incremental_read(
        CH_TABLE_NAME_HOLE, table_path, keeper_path=keeper_path
    )
    count_query = f"SELECT count() FROM {CH_TABLE_NAME_HOLE}"
    _drain_baseline(count_query, "1\n")

    # Snapshots 2, 3 and 4. Snapshot 1 stays, so earliest is 1 and the missing snapshot 3
    # sits above it - it cannot be explained by expiration.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=3)
    _warehouse_shell(writer_container_id, f"cp {snapshot_file} {snapshot_file}.bak")
    _warehouse_shell(writer_container_id, f"rm -f {snapshot_file}")

    zk = cluster.get_kazoo_client("zoo1")
    try:
        deadline = time.monotonic() + 60
        error = ""
        while time.monotonic() < deadline:
            error = node.query_and_get_answer_with_error(count_query)[1]
            if error:
                break
            time.sleep(0.5)

        assert error, "a missing snapshot above the earliest id was silently skipped"
        assert "snapshot 3" in error, (
            f"the error does not name the missing snapshot: {error!r}"
        )
        assert zk.get(f"{keeper_path}/committed_snapshot")[0] == b"1", (
            "the watermark advanced past a snapshot that was not expired"
        )

        # Put it back: nothing was lost, all three snapshots are delivered.
        _warehouse_shell(writer_container_id, f"mv {snapshot_file}.bak {snapshot_file}")
        _wait_until_query_result(count_query, "30\n", database="default", retries=120)
        assert zk.get(f"{keeper_path}/committed_snapshot")[0] == b"4"
    finally:
        zk.stop()

    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME_HOLE} SYNC;")


def test_paimon_to_mergetree_via_refresh_mv(started_cluster):
    """
    Validate the end-to-end pipeline:
      Paimon (incremental read) -> Refreshable MV (APPEND) -> MergeTree

    The refreshable MV periodically selects from the Paimon source table
    (which returns only new data each time) and appends to a MergeTree
    destination table.

    Prerequisites:
      - The Paimon source table must have paimon_metadata_refresh_interval_sec
        enabled so that new snapshots are picked up automatically between
        MV refresh cycles.
    """
    MV_REFRESH_INTERVAL_SEC = 10
    SLEEP_AFTER_WRITE_SEC = MV_REFRESH_INTERVAL_SEC + 5

    writer_container_id = cluster.get_instance_docker_id("paimon-incremental-writer")

    warehouse_name = "warehouse_mv"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{USER_FILES_PATH}/{warehouse_name}/test.db/test_table"

    _clean_warehouse(writer_container_id, warehouse_dir)

    # Warm-up commit: create initial Paimon snapshot so schema can be inferred.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)

    # Create Paimon source table with incremental read enabled.
    _create_clickhouse_table_for_paimon_incremental_read(CH_MV_PAIMON_TABLE, table_path)

    # Consume warm-up snapshot to establish incremental read baseline.
    _wait_until_query_result(
        f"SELECT count() FROM {CH_MV_PAIMON_TABLE}",
        "1\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_MV_PAIMON_TABLE}",
        "0\n",
        database="default",
    )

    # Create MergeTree destination table with same schema as Paimon table.
    node.query(f"DROP TABLE IF EXISTS {CH_MV_MERGETREE_TABLE} SYNC;")
    node.query(
        "CREATE TABLE {dest} AS {src} ENGINE = MergeTree() ORDER BY tuple()".format(
            dest=CH_MV_MERGETREE_TABLE, src=CH_MV_PAIMON_TABLE
        )
    )

    # Create refreshable MV in APPEND mode.
    node.query(f"DROP VIEW IF EXISTS {CH_MV_NAME} SYNC;")
    node.query(
        "CREATE MATERIALIZED VIEW {mv} "
        "REFRESH EVERY {interval} SECOND "
        "APPEND "
        "TO {dest} "
        "AS SELECT * FROM {src}".format(
            mv=CH_MV_NAME,
            interval=MV_REFRESH_INTERVAL_SEC,
            dest=CH_MV_MERGETREE_TABLE,
            src=CH_MV_PAIMON_TABLE,
        )
    )

    # --- First batch: write 10 rows to Paimon ---
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=1)

    time.sleep(SLEEP_AFTER_WRITE_SEC)

    result = node.query(f"SELECT count() FROM {CH_MV_MERGETREE_TABLE}")
    assert result == "10\n", f"Expected 10 rows after first refresh, got {result}"

    # --- Second batch: write another 10 rows to Paimon ---
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=11, rows_per_commit=10, commit_times=1)

    time.sleep(SLEEP_AFTER_WRITE_SEC)

    # MergeTree should accumulate to 20 rows total (APPEND mode).
    result = node.query(f"SELECT count() FROM {CH_MV_MERGETREE_TABLE}")
    assert result == "20\n", f"Expected 20 rows after second refresh, got {result}"

    # Cleanup: stop MV first to prevent background refresh from blocking DDL.
    node.query(f"SYSTEM STOP VIEW {CH_MV_NAME};")
    node.query(f"DROP VIEW IF EXISTS {CH_MV_NAME} SYNC;")
    node.query(f"DROP TABLE IF EXISTS {CH_MV_MERGETREE_TABLE} SYNC;")
    node.query(f"DROP TABLE IF EXISTS {CH_MV_PAIMON_TABLE} SYNC;")
