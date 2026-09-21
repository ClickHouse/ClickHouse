"""A read-only `Overlay` facade whose source is itself a read-only `Overlay`.

Every path that can configure such a pair rejects it up front, so the state is only reachable from
metadata that was written before those checks existed (or written while the facade was detached, as
here) and replayed at server startup. Startup deliberately keeps the definition alive -- a server
that does not start is far worse than a facade that loses a source -- and drops the nested source
from the union on every lookup instead.

`CREATE TABLE` through such a facade must fail closed rather than skip the nested source: the
documented target is the first *writable source database*, and the documented repair of the
definition (re-creating that source as an ordinary database) puts the source back into the facade
order, so a table created past it would afterwards be owned by the wrong source, or shadowed by a
table of the same name in the source the create walked past.

Related: https://github.com/ClickHouse/ClickHouse/pull/86768
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_create_table_through_facade_with_nested_source(start_cluster):
    node.query(
        """
        DROP DATABASE IF EXISTS db_top;
        DROP DATABASE IF EXISTS db_mid;
        DROP DATABASE IF EXISTS db_b;
        DROP DATABASE IF EXISTS db_src;

        CREATE DATABASE db_src ENGINE = Atomic;
        CREATE TABLE db_src.t_src (id UInt64) ENGINE = MergeTree ORDER BY id;
        INSERT INTO db_src.t_src VALUES (1), (2);

        CREATE DATABASE db_mid ENGINE = Atomic;
        CREATE DATABASE db_b ENGINE = Atomic;
        CREATE TABLE db_b.t_b (id UInt64) ENGINE = MergeTree ORDER BY id;
        INSERT INTO db_b.t_b VALUES (3);

        CREATE DATABASE db_top ENGINE = Overlay('db_mid', 'db_b');
        """
    )

    # While both sources are ordinary databases, the first one takes the table.
    node.query("CREATE TABLE db_top.t_new (id UInt64) ENGINE = MergeTree ORDER BY id")
    assert node.query("EXISTS TABLE db_mid.t_new").strip() == "1"
    assert node.query("EXISTS TABLE db_b.t_new").strip() == "0"

    # Turn the first source into a facade itself. Both directions of the nesting are rejected while
    # the facade is attached, so this models metadata that predates those checks: the facade is
    # detached (its metadata file stays on disk) while the source is re-created.
    node.query("DETACH DATABASE db_top")
    node.query("DROP DATABASE db_mid")
    node.query("CREATE DATABASE db_mid ENGINE = Overlay('db_src')")

    # Replaying the metadata must keep the server startable and the facade attached.
    node.restart_clickhouse()
    assert node.query("SELECT engine FROM system.databases WHERE name = 'db_top'").strip() == "Overlay"

    # The nested source contributes nothing to the union, so only the later source is visible.
    assert node.query("SELECT count() FROM db_top.t_b").strip() == "1"
    assert node.query("EXISTS TABLE db_top.t_src").strip() == "0"

    # `CREATE TABLE` must not silently land in the later source.
    error = node.query_and_get_error(
        "CREATE TABLE db_top.t_second (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    assert "BAD_ARGUMENTS" in error
    assert "cannot be used as a source" in error
    assert node.query("EXISTS TABLE db_b.t_second").strip() == "0"

    # The same holds for a name the nested source's own source already owns.
    error = node.query_and_get_error(
        "CREATE TABLE db_top.t_src (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    assert "BAD_ARGUMENTS" in error
    assert node.query("EXISTS TABLE db_b.t_src").strip() == "0"

    # The underlying databases still take tables directly.
    node.query("CREATE TABLE db_b.t_direct (id UInt64) ENGINE = MergeTree ORDER BY id")

    # Repairing the definition restores the documented behaviour: the first source takes the table.
    node.query("DROP DATABASE db_mid")
    node.query("CREATE DATABASE db_mid ENGINE = Atomic")
    node.query("CREATE TABLE db_top.t_third (id UInt64) ENGINE = MergeTree ORDER BY id")
    assert node.query("EXISTS TABLE db_mid.t_third").strip() == "1"
    assert node.query("EXISTS TABLE db_b.t_third").strip() == "0"

    node.query(
        """
        DROP DATABASE db_top;
        DROP DATABASE db_mid;
        DROP DATABASE db_b;
        DROP DATABASE db_src;
        """
    )
