"""Restart contract for a dependency edge that points at a removed object.

`DROP` / `DETACH ... PERMANENTLY` no longer refuse to finish when a dependent is registered
concurrently, after the object has already been shut down (see
`04648_detach_post_shutdown_dependency_recheck.sh`). The price is that the losing `CREATE` is left with
a dependency edge referencing an object that is gone.

That edge is *not* transient: the dependency graph is rebuilt from the dependent's metadata on every
startup (`TablesLoader::buildDependencyGraph`, then merged into `DatabaseCatalog`), and
`removeUnresolvableDependencies` only drops the missing name from the local loading-schedule graph.
So this test pins what that state does across a restart, which is the part that would otherwise be
assumed rather than verified.

It matters most with `async_load_databases = false`: there a failing table load takes the whole server
down, so if a dangling dependent could fail to *attach*, the change would be able to turn a race into a
server that will not restart. Both modes are therefore exercised.

The dangling state is produced by disabling the dependency checks rather than by winning the race --
the resulting state is identical, it is already reachable that way on any released version, and it
keeps this test free of failpoint choreography. The race itself is covered by the stateless test.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_async = cluster.add_instance(
    "node_async",
    main_configs=["configs/async_load_on.xml"],
    stay_alive=True,
)

node_sync = cluster.add_instance(
    "node_sync",
    main_configs=["configs/async_load_off.xml"],
    stay_alive=True,
)

NO_DEPENDENCY_CHECKS = {
    "check_table_dependencies": 0,
    "check_referential_table_dependencies": 0,
}


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_dictionary(node, database):
    node.query(
        f"""
        CREATE DICTIONARY {database}.dict (id UInt64, val String)
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 'src' DB '{database}'))
        LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)
        """
    )


@pytest.mark.parametrize("node_name", ["node_async", "node_sync"])
def test_dangling_dependency_survives_restart(started_cluster, node_name):
    node = started_cluster.instances[node_name]
    database = "dangling"

    node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node.query(f"CREATE DATABASE {database}")
    node.query(
        f"CREATE TABLE {database}.src (id UInt64, val String) ENGINE = MergeTree ORDER BY id"
    )
    node.query(f"INSERT INTO {database}.src VALUES (1, 'a')")
    create_dictionary(node, database)
    # The dependent references the dictionary from a DEFAULT expression, with an explicit column type --
    # which is what the stored metadata always carries, so ATTACH does not have to resolve `dictGet`.
    node.query(
        f"""
        CREATE TABLE {database}.dep (id UInt64, v String DEFAULT dictGetString('{database}.dict', 'val', id))
        ENGINE = MergeTree ORDER BY id
        """
    )
    # An unrelated table in the same database: its loading must not be affected.
    node.query(f"CREATE TABLE {database}.other (id UInt64) ENGINE = MergeTree ORDER BY id")
    node.query(f"INSERT INTO {database}.other VALUES (7)")

    node.query(f"DROP DICTIONARY {database}.dict", settings=NO_DEPENDENCY_CHECKS)

    node.restart_clickhouse()

    # 1. The server came back at all. With async_load_databases = false a failing load would have
    #    stopped startup, so simply getting an answer here is the load-bearing assertion.
    assert node.query("SELECT 1").strip() == "1"

    # 2. The unrelated table is intact.
    assert node.query(f"SELECT count() FROM {database}.other").strip() == "1"

    # 3. The dependent itself attached and is readable; only evaluating the unresolvable default fails.
    assert (
        node.query(
            f"SELECT count() FROM system.tables WHERE database = '{database}' AND name = 'dep'"
        ).strip()
        == "1"
    )
    assert node.query(f"SELECT count() FROM {database}.dep").strip() == "0"
    assert "dict" in node.query_and_get_error(f"INSERT INTO {database}.dep (id) VALUES (1)")

    # 4. The loader reported the unresolvable dependency instead of silently ignoring it.
    assert node.contains_in_log("but seems like that does not exist")

    # 5. The edge was reconstructed from `dep`'s metadata, not lost: recreating the dictionary under the
    #    same name makes it a real dependency again, so removing it is refused once more.
    create_dictionary(node, database)
    assert "HAVE_DEPENDENT_OBJECTS" in node.query_and_get_error(
        f"DROP DICTIONARY {database}.dict"
    )

    # 6. And dropping the dependent clears it.
    node.query(f"DROP TABLE {database}.dep")
    node.query(f"DROP DICTIONARY {database}.dict")

    node.query(f"DROP DATABASE {database} SYNC")
