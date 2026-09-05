"""A projection whose definition cannot be analyzed is skipped only at `LoadingStrictnessLevel::FORCE_ATTACH`.

That level is produced when the server loads its metadata at startup, so restarting a real server is the
only way to reach the skip: an explicit `ATTACH TABLE` runs one level lower and throws instead of
skipping, and `UNDROP TABLE` throws earlier still, while parsing the stored statement. Hence an
integration test rather than a stateless one.
"""

import pytest

from helpers.cluster import ClickHouseCluster

DICT_XML = "/etc/clickhouse-server/dictionaries/proj_dict.xml"
DICT_XML_BACKUP = "/tmp/proj_dict.xml.bak"
DICT_CSV = "/var/lib/clickhouse/user_files/proj_dict.csv"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    dictionaries=["configs/dictionaries/proj_dict.xml"],
    stay_alive=True,
)


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


def test_unavailable_projection_is_not_deleted_by_alter(started_cluster):
    node.exec_in_container(
        ["bash", "-c", f"printf 'key,val\\n0,100\\n1,101\\n' > {DICT_CSV}"]
    )
    node.exec_in_container(["cp", DICT_XML, DICT_XML_BACKUP])

    node.query("DROP DATABASE IF EXISTS dl SYNC")
    node.query("CREATE DATABASE dl")
    node.query(
        "CREATE TABLE dl.t (x UInt64,"
        " PROJECTION p (SELECT x, dictGet('proj_dict', 'val', x % 2) AS dv GROUP BY x, dv))"
        " ENGINE = MergeTree ORDER BY x"
    )
    node.query(
        "CREATE TABLE dl.t2 (x UInt64,"
        " PROJECTION p (SELECT x, dictGet('proj_dict', 'val', x % 2) AS dv GROUP BY x, dv),"
        " PROJECTION q (SELECT x, dictGet('proj_dict', 'val', x % 3) AS dv GROUP BY x, dv))"
        " ENGINE = MergeTree ORDER BY x"
    )
    node.query("INSERT INTO dl.t SELECT number FROM numbers(100)")
    node.query("INSERT INTO dl.t2 SELECT number FROM numbers(100)")

    # The fixture is armed: every declaration is analyzed and materialized.
    assert projections("t") == "1"
    assert projections("t2") == "2"
    assert active_projection_parts("t") == "1"
    assert active_projection_parts("t2") == "2"

    # Retiring the dictionary is what makes the declarations unanalyzable at the next startup, so it has
    # to happen before the restart or the rest of the test is vacuous.
    node.exec_in_container(["rm", DICT_XML])
    node.restart_clickhouse()

    # The skip fired, the server still started, and reads still work.
    assert projections("t") == "0"
    assert projections("t2") == "0"
    assert node.query("SELECT count() FROM dl.t").strip() == "100"
    assert node.query("SELECT count() FROM dl.t2").strip() == "100"

    # An ALTER would persist the reduced projection set, so it is refused while a declaration is
    # unanalyzable, and the message says how to get out.
    error = node.query_and_get_error("ALTER TABLE dl.t MODIFY COMMENT 'x'")
    assert "projection p is declared but could not be analyzed" in error
    assert "DROP PROJECTION" in error

    error = node.query_and_get_error(
        "ALTER TABLE dl.t2 ADD PROJECTION r (SELECT x GROUP BY x)"
    )
    assert "could not be analyzed" in error

    # Dropping is the documented way out, and it works one declaration at a time.
    node.query("ALTER TABLE dl.t2 DROP PROJECTION p")
    node.query("ALTER TABLE dl.t2 DROP PROJECTION q")
    assert "PROJECTION" not in node.query("SHOW CREATE TABLE dl.t2")
    node.query("ALTER TABLE dl.t2 MODIFY COMMENT 'y'")

    node.exec_in_container(["cp", DICT_XML_BACKUP, DICT_XML])
    node.restart_clickhouse()

    # `t` was never altered by the user, so its declaration and its materialized data come back.
    assert projections("t") == "1"
    assert "PROJECTION" in node.query("SHOW CREATE TABLE dl.t")
    assert active_projection_parts("t") == "1"
    assert node.query("SELECT count() FROM dl.t").strip() == "100"

    # `t2` stays without projections: the user dropped those declarations.
    assert projections("t2") == "0"
