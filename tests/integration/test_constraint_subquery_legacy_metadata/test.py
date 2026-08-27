"""A table read by a scalar subquery inside a CONSTRAINT is read while the dependent table is
being attached, so it is a loading dependency. Metadata written before the table names of the
expressions were qualified at CREATE time can contain unqualified names, and they have to
resolve against the database owning the table, both in the loading-dependency graph and in the
analysis run over the loaded metadata — not against the default database of the server, which
is unrelated to the table. The tests strip the qualifiers from the stored metadata, turning it
into the form written by the versions which did not qualify the names at CREATE time.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import get_database_disk_name, replace_text_in_metadata

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_metadata_path(database, table):
    return node.query(
        f"SELECT metadata_path FROM system.tables"
        f" WHERE database = '{database}' AND name = '{table}'"
    ).strip()


def restart_with_edited_metadata(metadata_path, old_value, new_value):
    replace_text_in_metadata(node, metadata_path, old_value, new_value)
    db_disk_name = get_database_disk_name(node)
    if db_disk_name != "default":
        node.query(f"SYSTEM CLEAR DISK METADATA CACHE {db_disk_name}")
    node.restart_clickhouse(kill=True)


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

    restart_with_edited_metadata(
        get_metadata_path("db1", "dependent"), "db1.source", "source"
    )

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


def test_legacy_unqualified_view_select(started_cluster):
    node.query("CREATE DATABASE db2")
    node.query(
        "CREATE TABLE db2.source (id UInt64) ENGINE = MergeTree ORDER BY tuple()"
    )
    node.query("INSERT INTO db2.source VALUES (42)")
    node.query("CREATE VIEW db2.v AS SELECT id FROM db2.source")

    restart_with_edited_metadata(
        get_metadata_path("db2", "v"), "db2.source", "source"
    )

    # The view SELECT persisted with a bare `FROM source` runs under the current database of the
    # reading query. It has been qualified with `db2`, which owns the view, when the metadata was
    # loaded, so reading the view from the `default` database works.
    assert node.query("SELECT * FROM db2.v") == "42\n"

    node.query("DROP TABLE db2.v")
    node.query("DROP TABLE db2.source")
    node.query("DROP DATABASE db2")


def test_legacy_unqualified_dictget_default(started_cluster):
    node.query("CREATE DATABASE db3")
    node.query(
        "CREATE TABLE db3.dict_source (key UInt64, value String)"
        " ENGINE = MergeTree ORDER BY key"
    )
    node.query("INSERT INTO db3.dict_source VALUES (1, 'one'), (2, 'two')")
    node.query(
        "CREATE DICTIONARY db3.dict (key UInt64, value String) PRIMARY KEY key"
        " SOURCE(CLICKHOUSE(DB 'db3' TABLE 'dict_source')) LIFETIME(0) LAYOUT(FLAT())"
    )
    # The default of `e` keeps its qualified dictionary name: it makes `db3.dict` a loading
    # dependency of the table, so the dictionary is registered before the table attaches and
    # the unqualified name in the default of `d` can be resolved while the metadata is loaded.
    node.query(
        "CREATE TABLE db3.t (x UInt64,"
        " d String DEFAULT dictGetString('db3.dict', 'value', x),"
        " e String DEFAULT dictGetString('db3.dict', 'value', x + 1))"
        " ENGINE = MergeTree ORDER BY tuple()"
    )

    restart_with_edited_metadata(
        get_metadata_path("db3", "t"),
        "dictGetString('db3.dict', 'value', x)",
        "dictGetString('dict', 'value', x)",
    )

    # The bare `dict` of the default of `d` has been qualified with `db3`, which owns the table,
    # when the metadata was loaded — not with the `default` database of the inserting query,
    # where no such dictionary exists.
    node.query("INSERT INTO db3.t (x) VALUES (1)", database="default")
    assert node.query("SELECT d, e FROM db3.t") == "one\ttwo\n"

    node.query("DROP TABLE db3.t")
    node.query("DROP DICTIONARY db3.dict")
    node.query("DROP TABLE db3.dict_source")
    node.query("DROP DATABASE db3")
