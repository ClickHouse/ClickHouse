import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,
    macros={"shard": 1, "replica": 1},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        # Both tests deliberately drive the digest assertion, which aborts the server.
        cluster.shutdown(ignore_logical_errors=True, ignore_fatal=True)


@pytest.fixture(autouse=True)
def running_server_with_the_assertion_compiled_in():
    # Every test here ends with the server aborted, and a failing one does not reach its own
    # cleanup, so bring the server back before deciding anything.
    if node.get_process_pid("clickhouse server") is None:
        node.start_clickhouse()
    if not (node.is_debug_build() or node.is_built_with_sanitizer()):
        pytest.skip("assertDigestWithProbability is compiled out in release builds")


def prepare_database(db):
    """Create a Replicated database with two tables. Each test uses its own database name, which
    is also its logger name, so the log assertions below cannot match another test's lines."""
    node.query(f"DROP DATABASE IF EXISTS {db} SYNC")
    node.query(
        f"CREATE DATABASE {db} ENGINE = Replicated('/clickhouse/databases/{db}', 'shard1', 'replica1')"
    )
    for name in ["diverging", "intact"]:
        node.query(
            f"CREATE TABLE {db}.{name} (n Int32) ENGINE = ReplicatedMergeTree ORDER BY n",
            settings={"distributed_ddl_task_timeout": 0},
        )
    return node.query(
        f"SELECT metadata_path FROM system.databases WHERE database='{db}'"
    ).strip()


def diverge_metadata_bytes_only(metadata_path):
    """Change a table's metadata bytes without changing what they parse to.

    A trailing newline is the whole mutation: both spellings parse to the same AST, so an AST
    comparison reports them equal while the digest, which hashes the raw bytes, diverges. The
    node uses a local database disk, so the metadata is a plain file.
    """
    node.exec_in_container(
        ["bash", "-c", f"printf '\\n' >> /var/lib/clickhouse/{metadata_path}"],
        user="root",
    )


def force_digest_check(db):
    node.query("SYSTEM ENABLE FAILPOINT database_replicated_force_metadata_digest_check")
    assert (
        node.query(
            "SELECT count() FROM system.fail_points WHERE enabled"
            " AND name = 'database_replicated_force_metadata_digest_check'"
        ).strip()
        == "1"
    ), "the failpoint did not arm, so nothing below would be evidence"
    # A DDL on the database samples the now-unconditional check. It aborts the server, which is
    # the point of the test, so losing the connection here is the expected outcome.
    node.query_and_get_error(
        f"CREATE TABLE {db}.probe (n Int32) ENGINE = ReplicatedMergeTree ORDER BY n",
        settings={"distributed_ddl_task_timeout": 0},
    )


def grep_table_report(db, table, tail):
    return node.grep_in_log(f"DatabaseReplicated ({db}): Table {table} (digest carrier: true): {tail}")


def test_digest_mismatch_names_the_diverging_table(started_cluster):
    db = "digest_diagnostic_names_table"
    metadata_path = prepare_database(db)
    diverge_metadata_bytes_only(f"{metadata_path}diverging.sql")

    force_digest_check(db)

    assert node.contains_in_log(
        f"DatabaseReplicated ({db}): Digest of local metadata"
    ), "the forced digest check did not detect the divergence"

    # The report must attribute the divergence to the table that carries it, and must not
    # accuse the intact one.
    assert grep_table_report(db, "diverging", "digest term").find("DIFFERS from coordinator") >= 0
    assert grep_table_report(db, "intact", "digest term").find("matches coordinator") >= 0

    # The AST comparison cannot see this divergence at all, which is why the raw byte comparison
    # above is the load-bearing part of the report rather than a nicety.
    assert node.contains_in_log(f"DatabaseReplicated ({db}): AST for table diverging is the same")

    # The terminating error is still the one CI matches this failure family on.
    assert node.contains_in_log("Digest does not match")

    node.start_clickhouse()
    node.query(f"DROP DATABASE IF EXISTS {db} SYNC")


def test_dump_survives_a_per_table_failure(started_cluster):
    """One unreportable table must not silence the report for the others.

    The dump runs immediately before an abort, so a whole-body catch would stop at the first
    unreportable table, naming neither it nor any table after it - and unreadable metadata is
    one of the divergences being hunted.
    """
    db = "digest_diagnostic_per_table"
    metadata_path = prepare_database(db)

    # Make the FIRST table in the (alphabetical) dump order unrenderable, and a LATER one carry
    # the actual digest divergence.
    zk = cluster.get_kazoo_client("zoo1")
    try:
        zk.set(f"/clickhouse/databases/{db}/metadata/diverging", b"this is not a CREATE query")
    finally:
        zk.stop()
    diverge_metadata_bytes_only(f"{metadata_path}intact.sql")

    force_digest_check(db)

    assert node.contains_in_log(f"DatabaseReplicated ({db}): Digest of local metadata")
    # The unrenderable table is named with an explicit error state...
    assert grep_table_report(db, "diverging", "cannot render metadata as AST")
    # ...and the dump still reached the table that comes after it.
    assert grep_table_report(db, "intact", "digest term").find("DIFFERS from coordinator") >= 0

    node.start_clickhouse()
    node.query(f"DROP DATABASE IF EXISTS {db} SYNC")
