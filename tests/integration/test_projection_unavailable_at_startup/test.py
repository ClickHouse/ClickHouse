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

    # Armed: every declaration is analyzed and materialized.
    assert projections("t") == "1"
    assert projections("t2") == "2"
    assert active_projection_parts("t") == "1"
    assert active_projection_parts("t2") == "2"

    node.restart_clickhouse()

    # The skip fired, the server still started, and reads still work. This is also the in-range control
    # for the recovery assertions at the end.
    assert projections("t") == "0"
    assert projections("t2") == "0"
    assert node.query("SELECT count() FROM dl.t").strip() == "100"
    assert node.query("SELECT count() FROM dl.t2").strip() == "100"

    # An ALTER would be validated against fewer projections than the table declares and would then
    # persist that reduced set, so it is refused while a declaration is unanalyzable.
    error = node.query_and_get_error("ALTER TABLE dl.t MODIFY COMMENT 'x'")
    assert "projection pp is declared but could not be analyzed" in error
    assert "DROP PROJECTION" in error
    assert "PROJECTION" in node.query("SHOW CREATE TABLE dl.t")
    assert declarations_on_disk("t") == "1"

    error = node.query_and_get_error(
        "ALTER TABLE dl.t2 ADD PROJECTION rr (SELECT a GROUP BY a)"
    )
    assert "could not be analyzed" in error

    # Dropping is the way out, and it works one declaration at a time: the one that was not dropped is
    # still declared in the statement this ALTER rewrote.
    node.query("ALTER TABLE dl.t2 DROP PROJECTION pp")
    assert "PROJECTION qq" in node.query("SHOW CREATE TABLE dl.t2")
    assert declarations_on_disk("t2") == "1"

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
    assert node.query("SELECT count() FROM dl.t").strip() == "100"

    # `t2` has no projections, because the user dropped those declarations.
    assert projections("t2") == "0"
