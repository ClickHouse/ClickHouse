"""A table read by a scalar subquery inside a CONSTRAINT is read while the dependent table is
being attached, so it is a loading dependency. Metadata written before the table names of the
constraint expressions were qualified at CREATE time can contain unqualified names, and they
have to resolve against the database owning the table, both in the loading-dependency graph
and in the constraint analysis of the attach itself — not against the default database of the
server, which is unrelated to the table.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_legacy_unqualified_constraint_subquery(started_cluster):
    node.query("CREATE DATABASE db1")
    node.query(
        "CREATE TABLE db1.source (id UInt64) ENGINE = MergeTree ORDER BY tuple()"
    )
    node.query("INSERT INTO db1.source VALUES (1)")
    node.query(
        "CREATE TABLE db1.dependent (x UInt64,"
        " CONSTRAINT c CHECK x < (SELECT max(id) + 1000 FROM db1.source))"
        " ENGINE = MergeTree ORDER BY tuple()"
    )

    # Strip the database qualifier from the stored metadata, turning it into the form written
    # by the versions which did not qualify the table names of constraint expressions.
    node.stop_clickhouse()
    node.exec_in_container(
        [
            "bash",
            "-c",
            r"sed -i 's/db1\.source/source/' /var/lib/clickhouse/metadata/db1/dependent.sql",
        ]
    )
    node.start_clickhouse()

    # The bare `FROM source` has resolved against `db1`, which owns the table, and not against
    # the `default` database of the server, where no `source` table exists: the table has
    # attached successfully and `db1.source` is its loading dependency.
    assert (
        node.query(
            "SELECT loading_dependencies_database, loading_dependencies_table"
            " FROM system.tables WHERE database = 'db1' AND name = 'dependent'"
        )
        == "['db1']\t['source']\n"
    )
    assert "HAVE_DEPENDENT_OBJECTS" in node.query_and_get_error("DROP TABLE db1.source")

    node.query("DROP TABLE db1.dependent")
    node.query("DROP TABLE db1.source")
    node.query("DROP DATABASE db1")
