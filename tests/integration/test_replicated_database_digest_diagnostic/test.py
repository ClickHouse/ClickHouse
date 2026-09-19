import re

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
        # Every test deliberately drives the digest assertion, which aborts the server.
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

    Returns the on-disk size AFTER the mutation, which pins every number the report must print.
    """
    node.exec_in_container(
        ["bash", "-c", f"printf '\\n' >> /var/lib/clickhouse/{metadata_path}"],
        user="root",
    )
    return int(
        node.exec_in_container(
            ["bash", "-c", f"stat -c %s /var/lib/clickhouse/{metadata_path}"],
            user="root",
        ).strip()
    )


DIGEST_ABORT = "Digest does not match"


def arm_failpoint(name):
    node.query(f"SYSTEM ENABLE FAILPOINT {name}")
    assert (
        node.query(
            f"SELECT count() FROM system.fail_points WHERE enabled AND name = '{name}'"
        ).strip()
        == "1"
    ), f"the failpoint {name} did not arm, so nothing below would be evidence"


def force_digest_check(db, extra_failpoints=()):
    """Drive the digest check and return the log counts captured just before it ran.

    The abort line comes from the root logger, so unlike every other assertion here it cannot be
    scoped to one database, and an earlier test in this module already put it in the log. Callers
    therefore have to assert a delta, which is what the returned counts are for.
    """
    arm_failpoint("database_replicated_force_metadata_digest_check")
    for name in extra_failpoints:
        arm_failpoint(name)
    before = {
        line: int(node.count_in_log(line))
        for line in [DIGEST_ABORT, "Got unexpected query from"]
    }
    # A DDL on the database samples the now-unconditional check. It aborts the server, which is
    # the point of the test, so losing the connection here is the expected outcome.
    node.query_and_get_error(
        f"CREATE TABLE {db}.probe (n Int32) ENGINE = ReplicatedMergeTree ORDER BY n",
        settings={"distributed_ddl_task_timeout": 0},
    )
    return before


def assert_log_delta(before, line, expected_more):
    """Assert how many NEW occurrences of `line` this test's own abort produced."""
    actual = int(node.count_in_log(line)) - before[line]
    if expected_more:
        assert actual > 0, f"expected new '{line}' lines, got {actual}"
    else:
        assert actual == 0, f"expected no new '{line}' lines, got {actual}"


def grep_table_report(db, table, tail):
    return node.grep_in_log(f"DatabaseReplicated ({db}): Table {table} (digest carrier: true): {tail}")


def dump_lines(db):
    """The dump's own per-table lines for this database, in the order they were logged."""
    return [
        line
        for line in node.grep_in_log(f"DatabaseReplicated ({db}): Table ").splitlines()
        if line.strip()
    ]


# Every field the report adds over the pre-existing AST-only dump. Asserting the whole payload,
# not just the verdict, is what makes dropping any single field fail this test.
DIFFERS_PAYLOAD = (
    r"digest term (\d{1,20}), raw metadata DIFFERS from coordinator "
    r"\(on disk (\d+) bytes, coordinator (\d+) bytes, first difference at byte (\d+), "
    r"coordinator term (\d{1,20})\)"
)
MATCHES_PAYLOAD = r"digest term (\d{1,20}), raw metadata matches coordinator"


def test_digest_mismatch_names_the_diverging_table(started_cluster):
    db = "digest_diagnostic_names_table"
    metadata_path = prepare_database(db)
    on_disk_size = diverge_metadata_bytes_only(f"{metadata_path}diverging.sql")

    before = force_digest_check(db)

    assert node.contains_in_log(
        f"DatabaseReplicated ({db}): Digest of local metadata"
    ), "the forced digest check did not detect the divergence"

    # The report must attribute the divergence to the table that carries it, with every field it
    # promises: both sizes, both digest terms, and where the bytes first differ. The mutation is
    # exactly one appended byte, so all four numbers are determined.
    differs = re.search(DIFFERS_PAYLOAD, grep_table_report(db, "diverging", "digest term"))
    assert differs, "the diverging table's report is missing or has lost a reported field"
    local_term, disk_bytes, coord_bytes, first_diff, coord_term = differs.groups()
    assert int(disk_bytes) == on_disk_size
    assert int(coord_bytes) == on_disk_size - 1
    assert int(first_diff) == on_disk_size - 1
    assert local_term != coord_term, "the two sides must hash differently, that is the divergence"

    # ...and must not accuse the intact one.
    matches = re.search(MATCHES_PAYLOAD, grep_table_report(db, "intact", "digest term"))
    assert matches, "the intact table must still be reported, with its digest term"

    # The AST comparison cannot see this divergence at all, which is why the raw byte comparison
    # above is the load-bearing part of the report rather than a nicety.
    assert node.contains_in_log(f"DatabaseReplicated ({db}): AST for table diverging is the same")

    # The terminating error is still the one CI matches this failure family on.
    assert_log_delta(before, DIGEST_ABORT, expected_more=True)

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

    before = force_digest_check(db)

    assert_log_delta(before, DIGEST_ABORT, expected_more=True)
    assert node.contains_in_log(f"DatabaseReplicated ({db}): Digest of local metadata")
    # The unrenderable table is named with an explicit error state, reported as the error CODE:
    # the parser's message embeds the metadata it failed on, and metadata is unmasked and can hold
    # secrets. Scoped to the dump's own line - other subsystems log that metadata on their own
    # error paths, which is outside this diff.
    render_failure = grep_table_report(db, "diverging", "cannot render metadata as AST")
    assert "SYNTAX_ERROR" in render_failure, render_failure
    assert "this is not a CREATE query" not in render_failure, render_failure

    # ...and the dump still reached the table that comes after it.
    assert grep_table_report(db, "intact", "digest term").find("DIFFERS from coordinator") >= 0

    # The byte report for EVERY table must precede the first rendering failure. Interleaving the
    # two would mean a table whose rendering fails costs the byte report of every table after it,
    # and the byte report is what names the diverging one.
    lines = dump_lines(db)
    first_failure = next(i for i, l in enumerate(lines) if "cannot render metadata as AST" in l)
    reported_before = {l.split(" Table ")[1].split(" ")[0] for l in lines[:first_failure] if "digest term" in l}
    assert reported_before >= {"diverging", "intact", "probe"}, sorted(reported_before)

    node.start_clickhouse()
    node.query(f"DROP DATABASE IF EXISTS {db} SYNC")


def test_dump_survives_metadata_that_is_not_in_the_expected_form(started_cluster):
    """Coordinator metadata that parses but is not in the form this database writes.

    `parseQueryFromMetadata` answers that case with a LOGICAL_ERROR, which aborts at construction
    in exactly the builds this dump runs in - so no catch inside the dump could contain it, and the
    process would die on a DIFFERENT fatal signature, losing both the table names and the family's
    own signature. The dump must therefore report the form itself and let the digest assertion be
    the thing that fires.
    """
    db = "digest_diagnostic_unexpected_form"
    metadata_path = prepare_database(db)

    # Parses cleanly, but carries no UUID and a real table name instead of the placeholder.
    node_path = f"/clickhouse/databases/{db}/metadata/diverging"
    zk = cluster.get_kazoo_client("zoo1")
    try:
        original_metadata = zk.get(node_path)[0]
        zk.set(node_path, b"CREATE TABLE diverging (n Int32) ENGINE = MergeTree ORDER BY n")
    finally:
        zk.stop()
    diverge_metadata_bytes_only(f"{metadata_path}intact.sql")

    before = force_digest_check(db)

    # The abort must still be the digest assertion, not the parse assertion.
    assert_log_delta(before, DIGEST_ABORT, expected_more=True)
    assert_log_delta(before, "Got unexpected query from", expected_more=False)

    assert grep_table_report(db, "diverging", "coordinator metadata is not in the expected form")
    # ...and the byte report for the table after it survived.
    assert grep_table_report(db, "intact", "digest term").find("DIFFERS from coordinator") >= 0

    # Startup parses coordinator metadata through the same helper, on a real execution path where
    # the unexpected form IS an assertion failure - so the node must be put back before restarting,
    # or the server aborts again during startup and never comes up.
    zk = cluster.get_kazoo_client("zoo1")
    try:
        zk.set(node_path, original_metadata)
    finally:
        zk.stop()

    node.start_clickhouse()
    node.query(f"DROP DATABASE IF EXISTS {db} SYNC")


def test_table_only_in_coordinator_gets_its_digest_term(started_cluster):
    """A table present in the coordinator and absent locally must report its digest term.

    That term is exactly the amount by which the coordinator digest exceeds the local recompute
    for this table, so it is the number that answers whether one table explains the whole delta.
    """
    db = "digest_diagnostic_coordinator_only"
    metadata_path = prepare_database(db)

    # Identical bytes under two names, so the reported terms must differ: the digest hashes the
    # table name together with the metadata. Removed again below, so no local table is ever
    # created from this metadata and the shared UUID cannot collide.
    ghost_metadata = (
        b"ATTACH TABLE _ UUID '00000000-1111-2222-3333-444444444444'"
        b" (n Int32) ENGINE = MergeTree ORDER BY n"
    )
    ghost_paths = [f"/clickhouse/databases/{db}/metadata/ghost_{i}" for i in (1, 2)]
    zk = cluster.get_kazoo_client("zoo1")
    try:
        for path in ghost_paths:
            zk.create(path, ghost_metadata)
    finally:
        zk.stop()
    diverge_metadata_bytes_only(f"{metadata_path}intact.sql")

    before = force_digest_check(db)

    assert_log_delta(before, DIGEST_ABORT, expected_more=True)

    terms = []
    for i in (1, 2):
        report = node.grep_in_log(
            f"DatabaseReplicated ({db}): Table ghost_{i} exists in ZK, but missing locally:"
        )
        found = re.search(
            r"coordinator digest term (\d{1,20}), (\d+) bytes in coordinator", report
        )
        assert found, f"ghost_{i} was named without its coordinator digest term: {report}"
        term, size = found.groups()
        assert int(size) == len(ghost_metadata)
        terms.append(term)
    assert terms[0] != terms[1], "the digest hashes the table name, so the terms must differ"

    # No local fields are invented for a table that has no local metadata.
    for i in (1, 2):
        assert not node.contains_in_log(f"DatabaseReplicated ({db}): Table ghost_{i} (digest carrier")

    zk = cluster.get_kazoo_client("zoo1")
    try:
        for path in ghost_paths:
            zk.delete(path)
    finally:
        zk.stop()

    node.start_clickhouse()
    node.query(f"DROP DATABASE IF EXISTS {db} SYNC")


def test_dump_reports_local_metadata_without_a_coordinator_snapshot(started_cluster):
    """A coordinator snapshot failure must not cost the local half of the report.

    The observed failure is the local recompute disagreeing with the in-memory counter, which
    reaches the dump without consulting the coordinator at all, and the lane where this family
    fires runs Keeper fault injection.
    """
    db = "digest_diagnostic_no_snapshot"
    metadata_path = prepare_database(db)
    on_disk_size = diverge_metadata_bytes_only(f"{metadata_path}diverging.sql")

    before = force_digest_check(
        db, extra_failpoints=["database_replicated_dump_fail_to_get_metadata_snapshot"]
    )

    assert_log_delta(before, DIGEST_ABORT, expected_more=True)
    snapshot_failure = node.grep_in_log(
        f"DatabaseReplicated ({db}): Cannot get the coordinator metadata snapshot"
    )
    assert "FAULT_INJECTED" in snapshot_failure, snapshot_failure

    # Every local table still gets its digest term and on-disk size, under wording that says the
    # coordinator side is unknown rather than claiming anything about it.
    for table, size in [("diverging", on_disk_size), ("intact", None)]:
        report = grep_table_report(db, table, "digest term")
        found = re.search(
            r"digest term (\d{1,20}), (\d+) bytes on disk, coordinator state unknown", report
        )
        assert found, f"{table} lost its local report without a snapshot: {report}"
        if size is not None:
            assert int(found.group(2)) == size

    # ...and nothing is accused of being absent from a coordinator that was never read. An empty
    # snapshot would otherwise make every table look missing, which is worse than the bug.
    assert not node.contains_in_log(f"DatabaseReplicated ({db}): Table diverging (digest carrier: true) exists locally, but is missing in coordinator")
    assert not node.contains_in_log(f"DatabaseReplicated ({db}): Table intact (digest carrier: true) exists locally, but is missing in coordinator")

    # The two coordinator-comparing passes have nothing to say, so they do not run.
    assert not node.contains_in_log(f"DatabaseReplicated ({db}): AST for table")
    assert not node.contains_in_log(f"DatabaseReplicated ({db}): AST differs for table")

    node.start_clickhouse()
    node.query(f"DROP DATABASE IF EXISTS {db} SYNC")


def test_dump_survives_a_std_logic_error(started_cluster):
    """The abort must still name the digest, even when the dump itself fails the worst way.

    Formatting a caught std::logic_error aborts in these builds, so reporting an escaped one with
    a formatting helper would replace `Digest does not match` with the dump's own signature -
    losing exactly the attribution this dump exists to provide.
    """
    db = "digest_diagnostic_logic_error"
    metadata_path = prepare_database(db)
    diverge_metadata_bytes_only(f"{metadata_path}diverging.sql")

    before = force_digest_check(
        db, extra_failpoints=["database_replicated_dump_throw_std_logic_error"]
    )

    # The injected failure is reported by error code, and the digest abort is still what aborts.
    handler_line = node.grep_in_log(
        "Failed to dump the local and coordinator metadata diff"
    )
    assert "STD_EXCEPTION" in handler_line, handler_line
    assert_log_delta(before, DIGEST_ABORT, expected_more=True)
    assert DIGEST_ABORT in node.grep_in_log("Logical error:")

    node.start_clickhouse()
    node.query(f"DROP DATABASE IF EXISTS {db} SYNC")
