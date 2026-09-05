"""A projection whose definition cannot be analyzed is skipped only at `LoadingStrictnessLevel::FORCE_ATTACH`.

That level is produced when the server loads its metadata at startup, so restarting a real server is the
only way to reach the skip: an explicit `ATTACH TABLE` runs one level lower and throws instead of
skipping, and `UNDROP TABLE` throws earlier still, while parsing the stored statement. Hence an
integration test rather than a stateless one.

`enable_positional_arguments_for_projections` defaults to false and is read from the query context when a
projection is analyzed, so a projection body written with positional arguments can be added while the
setting is on and then cannot be analyzed at any later startup. That is the state a server upgrade leaves
behind, and reaching it needs nothing removed from the machine.
"""

import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DATA_DIR = "/var/lib/clickhouse"
POSITIONAL_XML = "/etc/clickhouse-server/users.d/positional.xml"
POSITIONAL = {"enable_positional_arguments_for_projections": 1}

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def projections(table):
    return node.query(
        f"SELECT count() FROM system.projections WHERE database = 'dl' AND table = '{table}'"
    ).strip()


def active_projection_parts(table):
    return node.query(
        "SELECT count() FROM system.projection_parts"
        f" WHERE database = 'dl' AND table = '{table}' AND active"
    ).strip()


def declarations_on_disk(table):
    """How many projections the table's stored statement declares. The file holds one per line."""
    path = node.query(
        f"SELECT metadata_path FROM system.tables WHERE database = 'dl' AND name = '{table}'"
    ).strip()
    return node.exec_in_container(
        ["bash", "-c", f"grep -c 'PROJECTION ' {os.path.join(DATA_DIR, path)} || true"]
    ).strip()


def part_types(table):
    return node.query(
        "SELECT DISTINCT part_type FROM system.parts"
        f" WHERE database = 'dl' AND table = '{table}' AND active"
    ).strip()


def broken_projection_parts(table):
    return node.query(
        "SELECT count() FROM system.projection_parts"
        f" WHERE database = 'dl' AND table = '{table}' AND active AND is_broken"
    ).strip()


def check_table(table):
    return node.query(
        f"CHECK TABLE dl.{table}", settings={"check_query_single_value_result": 1}
    ).strip()


def event_value(event):
    """A ProfileEvent counter, 0 when the event has not fired since this server started."""
    return int(
        node.query(
            f"SELECT sum(value) FROM system.events WHERE event = '{event}'"
        ).strip()
    )


def test_unavailable_projection_is_not_deleted_by_alter(started_cluster):
    node.query("DROP DATABASE IF EXISTS dl SYNC")
    node.query("CREATE DATABASE dl")
    node.query("CREATE TABLE dl.t (a UInt64, b String) ENGINE = MergeTree ORDER BY a")
    node.query("CREATE TABLE dl.t2 (a UInt64, b String) ENGINE = MergeTree ORDER BY a")

    # The setting is what makes these declarations analyzable at all, so the fixture cannot be built
    # without it and this test cannot go vacuously green if it is ever retired.
    error = node.query_and_get_error(
        "ALTER TABLE dl.t ADD PROJECTION pp (SELECT b, a GROUP BY 1, 2)"
    )
    assert "not under aggregate function and not in GROUP BY keys" in error

    node.query(
        "ALTER TABLE dl.t ADD PROJECTION pp (SELECT b, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    node.query(
        "ALTER TABLE dl.t2 ADD PROJECTION pp (SELECT b, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    node.query(
        "ALTER TABLE dl.t2 ADD PROJECTION qq (SELECT a, b GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    node.query("INSERT INTO dl.t SELECT number, toString(number) FROM numbers(100)")
    node.query("INSERT INTO dl.t2 SELECT number, toString(number) FROM numbers(100)")

    # A wide part, so a mutation that touches one column hardlinks the files it was not told to skip
    # instead of rewriting the whole part.
    node.query(
        "CREATE TABLE dl.t3 (a UInt64, b String, c String) ENGINE = MergeTree ORDER BY a"
        " SETTINGS min_bytes_for_wide_part = 0"
    )
    node.query(
        "ALTER TABLE dl.t3 ADD PROJECTION pp (SELECT b, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    node.query(
        "INSERT INTO dl.t3 SELECT number, toString(number), '' FROM numbers(100)"
    )

    # Anti-vacuity for the mutation oracles below: a Compact part, or any full rewrite, drops `pp.proj`
    # for a reason this PR does not own.
    assert part_types("t3") == "Wide"

    # `t4` pairs an unanalyzable declaration with an analyzable one, so `DROP PROJECTION qq` is
    # allowed while `pp` is unavailable. Wide, so that mutation hardlinks what it was not told to skip.
    node.query(
        "CREATE TABLE dl.t4 (a UInt64, b String) ENGINE = MergeTree ORDER BY a"
        " SETTINGS min_bytes_for_wide_part = 0"
    )
    node.query(
        "ALTER TABLE dl.t4 ADD PROJECTION pp (SELECT b, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    # `qq` uses no positional arguments, so it can be analyzed without the setting; the setting is on
    # here only because this ALTER re-derives `pp` too (`AlterCommands::apply`), as `t2`'s do.
    node.query(
        "ALTER TABLE dl.t4 ADD PROJECTION qq (SELECT a, b GROUP BY a, b)",
        settings=POSITIONAL,
    )
    node.query("INSERT INTO dl.t4 SELECT number, toString(number) FROM numbers(100)")

    # Armed: every declaration is analyzed and materialized.
    assert projections("t") == "1"
    assert projections("t2") == "2"
    assert projections("t3") == "1"
    assert active_projection_parts("t") == "1"
    assert active_projection_parts("t2") == "2"
    assert active_projection_parts("t3") == "1"
    assert projections("t4") == "2"
    assert active_projection_parts("t4") == "2"
    assert part_types("t4") == "Wide"

    node.restart_clickhouse()

    # The skip fired, the server still started, and reads still work. This is also the in-range control
    # for the recovery assertions at the end.
    assert projections("t") == "0"
    assert projections("t2") == "0"
    assert projections("t3") == "0"
    assert projections("t4") == "1"  # only `qq` can be analyzed without the setting
    assert node.query("SELECT count() FROM dl.t").strip() == "100"
    assert node.query("SELECT count() FROM dl.t2").strip() == "100"

    # An ALTER is validated against fewer projections than the table declares, so one that invalidated
    # the unanalyzable declaration would be accepted; it is refused while such a declaration exists.
    error = node.query_and_get_error("ALTER TABLE dl.t MODIFY COMMENT 'x'")
    assert "projection pp is declared but could not be analyzed" in error
    assert "DROP PROJECTION" in error
    assert "PROJECTION" in node.query("SHOW CREATE TABLE dl.t")
    assert declarations_on_disk("t") == "1"

    # A mutation is not a metadata `ALTER`, so it is not refused. Nothing knows whether `pp`'s
    # materialized data still matches the rows it rewrites, so that data must be left out of the new
    # part rather than hardlinked into it and served as current after recovery.
    some_before, all_before = event_value("MutationSomePartColumns"), event_value(
        "MutationAllPartColumns"
    )
    node.query(
        "ALTER TABLE dl.t3 UPDATE b = 'z' WHERE a < 10", settings={"mutations_sync": 2}
    )
    assert event_value("MutationSomePartColumns") == some_before + 1
    assert event_value("MutationAllPartColumns") == all_before
    assert part_types("t3") == "Wide"

    # `DROP PROJECTION` is exempt, and dropping one projection rewrites no rows, so `pp`'s data must be
    # carried into the new part with its checksum entry intact: an entry whose directory is missing
    # registers a broken projection part, and one broken projection silences the whole data part's
    # consistency check on every later load.
    some_before, all_before = event_value("MutationSomePartColumns"), event_value(
        "MutationAllPartColumns"
    )
    node.query("ALTER TABLE dl.t4 DROP PROJECTION qq", settings={"mutations_sync": 2})
    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.mutations WHERE database = 'dl' AND table = 't4' AND NOT is_done",
        "0",
    )
    assert event_value("MutationSomePartColumns") == some_before + 1
    assert event_value("MutationAllPartColumns") == all_before
    assert declarations_on_disk("t4") == "1"
    assert projections("t4") == "0"

    error = node.query_and_get_error(
        "ALTER TABLE dl.t2 ADD PROJECTION rr (SELECT a GROUP BY a)"
    )
    assert "could not be analyzed" in error

    # Re-adding the same name hits the guard in `ProjectionsDescription::add`, which runs before the
    # blanket refusal: on master this silently replaced the declaration that is still on disk.
    error = node.query_and_get_error(
        "ALTER TABLE dl.t ADD PROJECTION pp (SELECT a GROUP BY a)"
    )
    assert "a projection with this name is declared but could not be analyzed" in error

    # Dropping is the way out, and it works one declaration at a time: the one that was not dropped is
    # still declared in the statement this ALTER rewrote.
    node.query("ALTER TABLE dl.t2 DROP PROJECTION pp")
    assert "PROJECTION qq" in node.query("SHOW CREATE TABLE dl.t2")
    assert declarations_on_disk("t2") == "1"

    # `CLEAR PROJECTION` carries the same command type as `DROP PROJECTION` and is exempt on purpose:
    # `AlterCommands::apply` deliberately keeps the declaration when `clear` is set, so it changes
    # projection data and no metadata that the unanalyzable declaration could be validated against.
    node.query("ALTER TABLE dl.t2 CLEAR PROJECTION qq")
    assert "PROJECTION qq" in node.query("SHOW CREATE TABLE dl.t2")

    node.query("ALTER TABLE dl.t2 DROP PROJECTION qq")
    assert "PROJECTION" not in node.query("SHOW CREATE TABLE dl.t2")
    node.query("ALTER TABLE dl.t2 MODIFY COMMENT 'y'")

    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/users.d/positional.xml"), POSITIONAL_XML
    )
    node.restart_clickhouse()

    # `t` was never altered by the user, so its declaration is still there to be analyzed once the
    # setting is back, and the projection data materialized before the restart is used as it is.
    assert projections("t") == "1"
    assert active_projection_parts("t") == "1"

    # The declaration coming back is only half the claim: the projection data written before the
    # restart must be readable. `force_optimize_projection_name` fails the query if `pp` is not used.
    assert (
        node.query(
            "SELECT sum(a) FROM (SELECT b, a FROM dl.t GROUP BY b, a)",
            settings={"force_optimize_projection_name": "pp"},
        ).strip()
        == "4950"
    )

    # `t2` has no projections, because the user dropped those declarations.
    assert projections("t2") == "0"

    # `t3`'s declaration survived, but the mutation rewrote the part that held the projection data, so
    # the projection comes back with nothing materialized instead of with pre-mutation rows.
    assert projections("t3") == "1"
    assert active_projection_parts("t3") == "0"
    assert broken_projection_parts("t3") == "0"
    assert check_table("t3") == "1"
    assert node.query("SELECT countIf(b = 'z') FROM dl.t3").strip() == "10"
    error = node.query_and_get_error(
        "SELECT countIf(b = 'z') FROM (SELECT b, a FROM dl.t3 GROUP BY b, a)",
        settings={"force_optimize_projection_name": "pp"},
    )
    assert "not used" in error

    # `t4`'s mutation rewrote no rows, so `pp` comes back with the data it already had, and the part it
    # lives in is still consistent.
    assert projections("t4") == "1"
    assert active_projection_parts("t4") == "1"
    assert broken_projection_parts("t4") == "0"
    assert check_table("t4") == "1"
    assert (
        node.query(
            "SELECT sum(a) FROM (SELECT b, a FROM dl.t4 GROUP BY b, a)",
            settings={"force_optimize_projection_name": "pp"},
        ).strip()
        == "4950"
    )
