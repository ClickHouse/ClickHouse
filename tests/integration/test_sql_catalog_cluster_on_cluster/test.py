"""
SQL catalog (`shards_catalog_storage` / `clusters_catalog_storage`) integration tests.

The lifecycle test is parametrized over all supported metadata backends so each path is exercised:
`keeper`, `keeper_encrypted`, `local`, `local_encrypted` (see `ClusterCatalogMetadataBackend.cpp`).
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry

DDL_CLUSTER = "integration_ddl_all"

# One full run per backend; configs under `configs/config.d/sql_catalog_cluster_metadata_*.xml`.
CATALOG_STORAGE_BACKENDS = (
    "keeper",
    "keeper_encrypted",
    "local",
    "local_encrypted",
)

METADATA_MAIN_CONFIGS = {
    "keeper": "configs/config.d/sql_catalog_cluster_metadata_keeper.xml",
    "keeper_encrypted": "configs/config.d/sql_catalog_cluster_metadata_keeper_encrypted.xml",
    "local": "configs/config.d/sql_catalog_cluster_metadata_local.xml",
    "local_encrypted": "configs/config.d/sql_catalog_cluster_metadata_local_encrypted.xml",
}


def assert_query_error_contains(node, sql, *needles):
    err = node.query_and_get_error(sql)
    for n in needles:
        assert n in err, (n, err)


def broadcast_catalog_ddl(initiator, broadcast, sql, *, with_sync=True):
    """
    Shard and cluster catalog rows are stored either in Keeper (replicated to peers) or on local disk.
    For local backends, the same DDL must run on every node via distributed DDL.

    Match `tmp/intg_manual_local_127_19000_19002.sql` and parsers in `ParserCreateShardQuery.cpp` /
    `ParserDropShardQuery.cpp`: for local broadcast, only `CREATE SHARD` / `CREATE CLUSTER` accept
    trailing `SYNC` after `ON CLUSTER`. `DROP SHARD` / `DROP CLUSTER` and all `ALTER SHARD` /
    `ALTER CLUSTER` must use `ON CLUSTER ...` without `SYNC`.
    """
    s = sql.strip().rstrip(";")
    if broadcast:
        suffix = f" ON CLUSTER {DDL_CLUSTER} SYNC" if with_sync else f" ON CLUSTER {DDL_CLUSTER}"
        initiator.query(f"{s}{suffix}")
    else:
        initiator.query(sql)


def drop_sql_catalog_topology(initiator, broadcast, names):
    """Best-effort teardown."""
    initiator.query(
        f"DROP TABLE IF EXISTS default.{names['workload_distr']} ON CLUSTER {names['cluster']} SYNC",
        ignore_error=True,
    )
    initiator.query(
        f"DROP TABLE IF EXISTS default.{names['workload_local']} ON CLUSTER {names['cluster']} SYNC",
        ignore_error=True,
    )
    broadcast_catalog_ddl(
        initiator,
        broadcast,
        f"DROP CLUSTER IF EXISTS {names['cluster']}",
        with_sync=False,
    )
    broadcast_catalog_ddl(
        initiator,
        broadcast,
        f"DROP SHARD IF EXISTS {names['shard_extra']}",
        with_sync=False,
    )
    broadcast_catalog_ddl(
        initiator,
        broadcast,
        f"DROP SHARD IF EXISTS {names['shard_ha']}",
        with_sync=False,
    )
    initiator.query(f"DROP NAMED COLLECTION IF EXISTS {names['rep_extra']}", ignore_error=True)
    initiator.query(f"DROP NAMED COLLECTION IF EXISTS {names['rep3']}", ignore_error=True)
    initiator.query(f"DROP NAMED COLLECTION IF EXISTS {names['rep2']}", ignore_error=True)
    initiator.query(f"DROP NAMED COLLECTION IF EXISTS {names['rep1']}", ignore_error=True)


def recreate_workload_tables(initiator, cluster_name, workload_local, workload_distr, sharding_key="rand()"):
    initiator.query(
        f"DROP TABLE IF EXISTS default.{workload_distr} ON CLUSTER {cluster_name} SYNC",
        ignore_error=True,
    )
    initiator.query(
        f"DROP TABLE IF EXISTS default.{workload_local} ON CLUSTER {cluster_name} SYNC",
        ignore_error=True,
    )
    initiator.query(
        f"""
        CREATE TABLE default.{workload_local} ON CLUSTER {cluster_name}
        (
            `x` UInt64
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/default/{workload_local}', '{{replica}}')
        ORDER BY x
        """
    )
    initiator.query(
        f"""
        CREATE TABLE default.{workload_distr} ON CLUSTER {cluster_name}
        (
            `x` UInt64
        )
        ENGINE = Distributed('{cluster_name}', 'default', '{workload_local}', {sharding_key})
        """
    )


def assert_distr_rowcount_all_nodes(nodes, workload_distr, expected: str):
    for node in nodes:
        assert_eq_with_retry(
            node,
            f"SELECT count() FROM default.{workload_distr} FORMAT TabSeparated",
            expected,
        )


def assert_workload_tables_on_nodes(nodes, workload_local, workload_distr):
    """
    After `CREATE TABLE ... ON CLUSTER`, each host that participates in the SQL catalog cluster for this DDL
    should see exactly two tables in `default` (`workload_local` + `workload_distr`). Catches replication / DDL lag
    before inserts and `SELECT count()` assertions.
    """
    for node in nodes:
        assert_eq_with_retry(
            node,
            f"SELECT count() FROM system.tables WHERE database = 'default' "
            f"AND name IN ('{workload_local}', '{workload_distr}') FORMAT TabSeparated",
            "2\n",
            retry_count=60,
            sleep_time=0.5,
        )


@pytest.mark.parametrize(
    "catalog_storage",
    [pytest.param(b, id=b) for b in CATALOG_STORAGE_BACKENDS],
)
def test_sql_catalog_cluster_metadata_storage_lifecycle(catalog_storage):
    """
    For each shards/clusters catalog storage backend (`keeper`, `keeper_encrypted`, `local`, `local_encrypted`):

    1) `CREATE NAMED COLLECTION` (ZK-backed), `CREATE SHARD`, `CREATE CLUSTER`, then `Distributed` inserts and reads.
       (`CREATE REPLICA` is covered in a follow-up commit once the surface is complete.)
    2) `ALTER SHARD` add/drop replica and `ALTER CLUSTER` add/drop shard; verify row counts after each topology change.
       The extra logical shard uses a third host (`ch3`) so `ON CLUSTER` does not see duplicate (host, port) pairs
       (Code 371 `INCONSISTENT_CLUSTER_DEFINITION` in `DDLTask::tryFindHostInCluster`).
    3) `ALTER SHARD` shard-level `MODIFY PROPERTIES` and replica endpoint change via `ALTER NAMED COLLECTION`
       (`ALTER SHARD ... MODIFY REPLICA` is not implemented yet), then verify `system.shards` / `system.clusters`.
    """
    broadcast = catalog_storage in ("local", "local_encrypted")
    names = {
        "rep1": f"intg_{catalog_storage}_r1",
        "rep2": f"intg_{catalog_storage}_r2",
        "rep3": f"intg_{catalog_storage}_r3",
        "rep_extra": f"intg_{catalog_storage}_r_extra",
        "shard_ha": f"intg_{catalog_storage}_shard_ha",
        "shard_extra": f"intg_{catalog_storage}_shard_single",
        "cluster": f"intg_{catalog_storage}_cluster",
        "workload_local": f"intg_{catalog_storage}_wl_local",
        "workload_distr": f"intg_{catalog_storage}_wl_distr",
    }

    main_configs = [
        "configs/config.d/sql_catalog_cluster_common.xml",
        METADATA_MAIN_CONFIGS[catalog_storage],
    ]

    cluster = ClickHouseCluster(__file__, name=f"sccmm_{catalog_storage}")
    ch1 = cluster.add_instance(
        "ch1",
        main_configs=main_configs,
        macros={"shard": "1", "replica": "r1"},
        with_zookeeper=True,
    )
    ch2 = cluster.add_instance(
        "ch2",
        main_configs=main_configs,
        macros={"shard": "2", "replica": "r1"},
        with_zookeeper=True,
    )
    ch3 = cluster.add_instance(
        "ch3",
        main_configs=main_configs,
        macros={"shard": "3", "replica": "r1"},
        with_zookeeper=True,
    )

    try:
        cluster.start()
    except Exception:
        cluster.shutdown()
        raise

    h1, h2, h3 = ch1.hostname, ch2.hostname, ch3.hostname
    port = 9000
    nodes_all = (ch1, ch2, ch3)
    # `ON CLUSTER {cluster}` only runs DDL on hosts listed for that cluster. Until `ch3` joins
    # (extra replica or second shard), workload tables do not exist on `ch3`.
    nodes_ha_two = (ch1, ch2)

    try:
        # --- (1) Bootstrap endpoints as named collections (ZK-backed; create on one node only). ---
        ch1.query(
            f"CREATE NAMED COLLECTION {names['rep1']} AS host = '{h1}', port = {port}, user = 'default'"
        )
        ch1.query(
            f"CREATE NAMED COLLECTION {names['rep2']} AS host = '{h2}', port = {port}, user = 'default'"
        )
        ch1.query(
            f"CREATE NAMED COLLECTION {names['rep3']} AS host = '{h3}', port = {port}, user = 'default'"
        )

        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"CREATE SHARD {names['shard_ha']} REPLICA {names['rep1']}, {names['rep2']} "
            f"PROPERTIES weight = 2, internal_replication = false",
        )
        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"CREATE CLUSTER {names['cluster']} ({names['shard_ha']}) PROPERTIES allow_distributed_ddl_queries = true",
        )
        # Shape: 1 shard total -> `shard_ha` with replicas [rep1(ch1), rep2(ch2)].

        assert_eq_with_retry(
            ch3,
            f"SELECT count() FROM system.clusters WHERE cluster = '{names['cluster']}' FORMAT TabSeparated",
            "2\n",
        )

        recreate_workload_tables(
            ch1, names["cluster"], names["workload_local"], names["workload_distr"]
        )
        assert_workload_tables_on_nodes(nodes_ha_two, names["workload_local"], names["workload_distr"])
        ch1.query(f"INSERT INTO default.{names['workload_distr']} SELECT number FROM numbers(17)")
        assert_distr_rowcount_all_nodes(nodes_ha_two, names["workload_distr"], "17\n")
        assert TSV(
            ch1.query(
                f"SELECT min(x), max(x), count() FROM default.{names['workload_distr']} FORMAT TabSeparated"
            )
        ) == TSV([[0, 16, 17]])

        # --- (2) Add / drop replica on the HA shard; then add / drop an extra shard; verify data each time. ---
        ch1.query(
            f"CREATE NAMED COLLECTION {names['rep_extra']} AS host = '{h3}', port = {port}, user = 'default'"
        )
        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"ALTER SHARD {names['shard_ha']} ADD REPLICA {names['rep_extra']}",
            with_sync=False,
        )
        # Shape: still 1 shard -> `shard_ha` now has 3 replicas [rep1(ch1), rep2(ch2), rep_extra(ch3)].
        rep_row = ch1.query(
            f"SELECT toString(replica_collections) FROM system.shards WHERE name = '{names['shard_ha']}' "
            f"FORMAT TabSeparated"
        ).strip()
        assert names["rep_extra"] in rep_row
        # `ON CLUSTER` is dispatched to every host in `integration_ddl_all` (ch1–ch3). On each node,
        # `DDLTask::tryFindHostInCluster` needs an exact `host_name` + port match for the local host in
        # `tryGetCluster({cluster})` (see `DDLTask.cpp`). Wait until the SQL catalog is replicated everywhere;
        # row count alone can race ahead of the host row `ch3` needs for matching.
        for node in (ch1, ch2, ch3):
            assert_eq_with_retry(
                node,
                f"SELECT count() FROM system.clusters WHERE cluster = '{names['cluster']}' FORMAT TabSeparated",
                "3\n",
                retry_count=60,
                sleep_time=0.5,
            )
        assert_eq_with_retry(
            ch3,
            f"SELECT count() FROM system.clusters WHERE cluster = '{names['cluster']}' AND host_name = '{h3}' FORMAT TabSeparated",
            "1\n",
            retry_count=60,
            sleep_time=0.5,
        )

        recreate_workload_tables(
            ch1, names["cluster"], names["workload_local"], names["workload_distr"]
        )
        assert_workload_tables_on_nodes(nodes_all, names["workload_local"], names["workload_distr"])
        ch1.query(f"INSERT INTO default.{names['workload_distr']} SELECT number + 1000 FROM numbers(23)")
        assert_distr_rowcount_all_nodes(nodes_all, names["workload_distr"], "23\n")

        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"ALTER SHARD {names['shard_ha']} DROP REPLICA {names['rep_extra']}",
            with_sync=False,
        )
        # Shape: still 1 shard -> back to 2 replicas [rep1(ch1), rep2(ch2)].
        rep_row = ch1.query(
            f"SELECT toString(replica_collections) FROM system.shards WHERE name = '{names['shard_ha']}' "
            f"FORMAT TabSeparated"
        ).strip()
        assert names["rep_extra"] not in rep_row

        recreate_workload_tables(
            ch1, names["cluster"], names["workload_local"], names["workload_distr"]
        )
        assert_workload_tables_on_nodes(nodes_ha_two, names["workload_local"], names["workload_distr"])
        ch1.query(f"INSERT INTO default.{names['workload_distr']} SELECT number + 2000 FROM numbers(23)")
        assert_distr_rowcount_all_nodes(nodes_ha_two, names["workload_distr"], "23\n")
        assert_eq_with_retry(
            ch1,
            f"SELECT count() FROM clusterAllReplicas('{names['cluster']}', 'default', '{names['workload_local']}') FORMAT TabSeparated",
            "46\n",
        )

        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"CREATE SHARD {names['shard_extra']} ({names['rep3']}) PROPERTIES weight = 1, internal_replication = false",
        )
        # Shape after CREATE SHARD only: catalog has 2 shards, but cluster still references only `shard_ha`.
        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"ALTER CLUSTER {names['cluster']} ADD SHARD {names['shard_extra']}",
            with_sync=False,
        )
        # Shape: 2 shards in cluster -> `shard_ha` (rep1, rep2) + `shard_extra` (rep3/ch3).
        assert_eq_with_retry(
            ch3,
            f"SELECT count() FROM system.clusters WHERE cluster = '{names['cluster']}' FORMAT TabSeparated",
            "3\n",
        )

        recreate_workload_tables(
            ch1, names["cluster"], names["workload_local"], names["workload_distr"]
        )
        assert_workload_tables_on_nodes(nodes_all, names["workload_local"], names["workload_distr"])
        ch1.query(f"INSERT INTO default.{names['workload_distr']} SELECT number + 100 FROM numbers(40)")
        assert_distr_rowcount_all_nodes(nodes_all, names["workload_distr"], "40\n")
        single_shard_rows_before_drop = ch1.query(
            f"SELECT count() FROM default.{names['workload_local']} FORMAT TabSeparated"
        )

        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"ALTER CLUSTER {names['cluster']} DROP SHARD {names['shard_extra']}",
            with_sync=False,
        )
        # Shape: `shard_extra` removed from cluster, back to 1 shard -> `shard_ha` with replicas [rep1, rep2].
        assert_eq_with_retry(
            ch3,
            f"SELECT count() FROM system.clusters WHERE cluster = '{names['cluster']}' FORMAT TabSeparated",
            "2\n",
        )
        assert_eq_with_retry(
            ch1,
            f"SELECT count() FROM default.{names['workload_distr']} FORMAT TabSeparated",
            single_shard_rows_before_drop,
        )
        assert_eq_with_retry(
            ch2,
            f"SELECT count() FROM default.{names['workload_distr']} FORMAT TabSeparated",
            single_shard_rows_before_drop,
        )

        recreate_workload_tables(
            ch1, names["cluster"], names["workload_local"], names["workload_distr"]
        )
        assert_workload_tables_on_nodes(nodes_ha_two, names["workload_local"], names["workload_distr"])
        ch1.query(f"INSERT INTO default.{names['workload_distr']} SELECT number + 200 FROM numbers(9)")
        assert_distr_rowcount_all_nodes(nodes_ha_two, names["workload_distr"], "9\n")

        # --- (3) Shard-level and replica-level (named collection) property changes. ---
        # Set HA shard weight to 1 so both shards can be compared under equal weights.
        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"ALTER SHARD {names['shard_ha']} MODIFY PROPERTIES (weight = 1, internal_replication = false)",
            with_sync=False,
        )
        # Add the extra shard back to the cluster, then set its weight to 1 as well.
        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"ALTER CLUSTER {names['cluster']} ADD SHARD {names['shard_extra']}",
            with_sync=False,
        )
        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"ALTER SHARD {names['shard_extra']} MODIFY PROPERTIES (weight = 1, internal_replication = false)",
            with_sync=False,
        )
        assert_eq_with_retry(
            ch2,
            f"SELECT weight, internal_replication FROM system.shards WHERE name = '{names['shard_ha']}' "
            f"FORMAT TabSeparated",
            "1\t0\n",
        )
        assert_eq_with_retry(
            ch3,
            f"SELECT count() FROM system.clusters WHERE cluster = '{names['cluster']}' FORMAT TabSeparated",
            "3\n",
        )
        assert_eq_with_retry(
            ch1,
            f"SELECT weight, internal_replication FROM system.shards WHERE name = '{names['shard_extra']}' "
            f"FORMAT TabSeparated",
            "1\t0\n",
        )
        assert "weight = 1" in ch1.query(f"SHOW CREATE SHARD {names['shard_ha']}")

        # Recreate tables using deterministic sharding (`x`) to make a strict 1:1 split assertion stable.
        recreate_workload_tables(
            ch1,
            names["cluster"],
            names["workload_local"],
            names["workload_distr"],
            sharding_key="x",
        )
        assert_workload_tables_on_nodes(nodes_all, names["workload_local"], names["workload_distr"])
        ch1.query(f"INSERT INTO default.{names['workload_distr']} SELECT number FROM numbers(200)")
        assert_distr_rowcount_all_nodes(nodes_all, names["workload_distr"], "200\n")
        # With two shards of equal weight, 200 sequential keys should split evenly: 100 rows per shard.
        assert_eq_with_retry(
            ch1,
            f"SELECT count() FROM default.{names['workload_local']} FORMAT TabSeparated",
            "100\n",
        )
        assert_eq_with_retry(
            ch2,
            f"SELECT count() FROM default.{names['workload_local']} FORMAT TabSeparated",
            "100\n",
        )
        assert_eq_with_retry(
            ch3,
            f"SELECT count() FROM default.{names['workload_local']} FORMAT TabSeparated",
            "100\n",
        )

        # Validate `internal_replication = true` behavior on a single-shard cluster:
        # after dropping `shard_extra`, a Distributed insert should target only one replica of `shard_ha`.
        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"ALTER SHARD {names['shard_ha']} MODIFY PROPERTIES (internal_replication = true)",
            with_sync=False,
        )
        assert_eq_with_retry(
            ch2,
            f"SELECT internal_replication FROM system.shards WHERE name = '{names['shard_ha']}' FORMAT TabSeparated",
            "1\n",
        )
        recreate_workload_tables(
            ch1, names["cluster"], names["workload_local"], names["workload_distr"]
        )
        assert_workload_tables_on_nodes(nodes_all, names["workload_local"], names["workload_distr"])
        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"ALTER CLUSTER {names['cluster']} DROP SHARD {names['shard_extra']}",
            with_sync=False,
        )
        assert_eq_with_retry(
            ch3,
            f"SELECT count() FROM system.clusters WHERE cluster = '{names['cluster']}' FORMAT TabSeparated",
            "2\n",
        )
        ch1.query(f"INSERT INTO default.{names['workload_distr']} SELECT number + 3000 FROM numbers(31)")

        local_rows_ch1 = ch1.query(
            f"SELECT count() FROM default.{names['workload_local']} FORMAT TabSeparated"
        ).strip()
        local_rows_ch2 = ch2.query(
            f"SELECT count() FROM default.{names['workload_local']} FORMAT TabSeparated"
        ).strip()
        assert sorted([local_rows_ch1, local_rows_ch2]) == ["0", "31"]

        # Validate `ALTER CLUSTER ... REPLACE shard_extra TO shard_ha` using data visibility:
        # 1) bind cluster to `shard_ha` and write 10 rows;
        # 2) switch cluster to `shard_extra` and write 20 rows there;
        # 3) replace `shard_extra` with `shard_ha` and check reads return the original 10 rows.
        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"ALTER SHARD {names['shard_ha']} MODIFY PROPERTIES (internal_replication = false)",
            with_sync=False,
        )
        recreate_workload_tables(
            ch1, names["cluster"], names["workload_local"], names["workload_distr"]
        )
        assert_workload_tables_on_nodes(nodes_ha_two, names["workload_local"], names["workload_distr"])
        ch1.query(f"INSERT INTO default.{names['workload_distr']} SELECT number + 4000 FROM numbers(10)")
        # Cluster is still `shard_ha` only here; `ADD SHARD` is below. Same as the first `17` row check.
        assert_distr_rowcount_all_nodes(nodes_ha_two, names["workload_distr"], "10\n")

        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"ALTER CLUSTER {names['cluster']} ADD SHARD {names['shard_extra']}",
            with_sync=False,
        )
        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"ALTER CLUSTER {names['cluster']} DROP SHARD {names['shard_ha']}",
            with_sync=False,
        )
        assert_eq_with_retry(
            ch3,
            f"SELECT count() FROM system.clusters WHERE cluster = '{names['cluster']}' FORMAT TabSeparated",
            "1\n",
        )
        recreate_workload_tables(
            ch1, names["cluster"], names["workload_local"], names["workload_distr"]
        )
        # Only `shard_extra` (ch3) is in the cluster; `ON CLUSTER` creates tables on ch3 only.
        assert_workload_tables_on_nodes((ch3,), names["workload_local"], names["workload_distr"])
        ch1.query(f"INSERT INTO default.{names['workload_distr']} SELECT number + 5000 FROM numbers(20)")
        assert_eq_with_retry(
            ch3,
            f"SELECT count() FROM default.{names['workload_local']} FORMAT TabSeparated",
            "20\n",
        )

        broadcast_catalog_ddl(
            ch1,
            broadcast,
            f"ALTER CLUSTER {names['cluster']} REPLACE {names['shard_extra']} TO {names['shard_ha']}",
            with_sync=False,
        )
        assert_eq_with_retry(
            ch3,
            f"SELECT count() FROM default.{names['workload_distr']} FORMAT TabSeparated",
            "10\n",
        )

        # Referential integrity (same spirit as `04071_sql_catalog_shard_cluster.sql`).
        assert_query_error_contains(
            ch1,
            f"DROP NAMED COLLECTION {names['rep1']}",
            "NAMED_COLLECTION_IS_REFERENCED",
        )
        assert_query_error_contains(
            ch1,
            f"DROP SHARD {names['shard_ha']}",
            "SHARD_IS_REFERENCED",
        )
    finally:
        drop_sql_catalog_topology(ch1, broadcast, names)
        cluster.shutdown()
