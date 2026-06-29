"""
SQL cluster catalog integration tests (`CREATE` / `ALTER` / `DROP` for ENDPOINT, SHARD, CLUSTER).

Cluster-catalog metadata requires Keeper-backed `cluster_metadata` in the server config.
Parametrized over `<encrypted>false</encrypted>` and `<encrypted>true</encrypted>`.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry as _assert_eq_with_retry

RETRY_COUNT = 3
RETRY_SLEEP_TIME = 0.5

# Catalog DDL enqueue is serialized through an ephemeral `counter_lock`; an initiator that loses the
# race gets `UNFINISHED` ("Client should retry") instead of blocking, so every concurrent statement
# must be retried. These substrings classify a query outcome as retryable contention vs a definitive
# "already exists" rejection vs a real failure.
CATALOG_RETRYABLE_ERRORS = (
    "UNFINISHED",
    "Client should retry",
    "Coordination::Exception",
    "Session expired",
    "ZooKeeper session expired",
    "Connection refused",
    "Connection reset",
    "Timeout",
    "KEEPER_EXCEPTION",
)
CATALOG_ALREADY_EXISTS = "already exists"


def assert_eq_with_retry(instance, query, expectation, **kwargs):
    kwargs.setdefault("retry_count", RETRY_COUNT)
    kwargs.setdefault("sleep_time", RETRY_SLEEP_TIME)
    return _assert_eq_with_retry(instance, query, expectation, **kwargs)


ENCRYPTED = (
    "false",
    "true",
)

ENCRYPTED_MAIN_CONFIGS = {
    "false": "configs/config.d/sql_catalog_replica_ddl_encrypted_false.xml",
    "true": "configs/config.d/sql_catalog_replica_ddl_encrypted_true.xml",
}


def assert_query_error_contains(node, sql, *needles):
    err = node.query_and_get_error(sql)
    for n in needles:
        assert n in err, (n, err)


def drop_endpoint_safely(initiator, *names):
    for name in names:
        initiator.query(f"DROP ENDPOINT IF EXISTS {name}", ignore_error=True)


def drop_catalog_topology_safely(initiator, *, clusters=(), shards=(), endpoints=()):
    for name in clusters:
        initiator.query(f"DROP CLUSTER IF EXISTS {name}", ignore_error=True)
    for name in shards:
        initiator.query(f"DROP SHARD IF EXISTS {name}", ignore_error=True)
    drop_endpoint_safely(initiator, *endpoints)


def catalog_query_with_retry(node, sql, *, attempts=80, sleep_time=0.25):
    """
    Execute one catalog DDL statement, transparently retrying the `counter_lock` contention error
    (`UNFINISHED` / "Client should retry") and transient Keeper hiccups.

    Returns "ok" if the statement committed, or "exists" if it was definitively rejected because the
    entity already exists (a normal outcome when several threads create the same name). Any other
    error is re-raised. Raises `AssertionError` if retries are exhausted (would indicate a wedge).
    """
    last_err = None
    for _ in range(attempts):
        try:
            node.query(sql)
            return "ok"
        except Exception as e:  # noqa: BLE001 - we classify by message below
            msg = str(e)
            if CATALOG_ALREADY_EXISTS in msg:
                return "exists"
            if any(token in msg for token in CATALOG_RETRYABLE_ERRORS):
                last_err = msg
                time.sleep(sleep_time)
                continue
            raise
    raise AssertionError(
        f"catalog_query_with_retry exhausted {attempts} attempts for: {sql}\nlast error: {last_err}"
    )


def run_concurrent_catalog_ddl(jobs, *, workers=16):
    """
    Run `jobs` (an iterable of `(key, node, sql)`) concurrently and return `{key: outcome}` where
    outcome is "ok" / "exists". Exceptions from worker threads propagate to the caller.
    """
    results = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_key = {
            executor.submit(catalog_query_with_retry, node, sql): key for key, node, sql in jobs
        }
        for future in as_completed(future_to_key):
            results[future_to_key[future]] = future.result()
    return results


def run_catalog_script_with_retry(node, script, *, attempts=400, sleep_time=0.5, timeout=900):
    """
    Run a multi-statement catalog DDL `script` (fed via stdin, so clickhouse-client executes every
    `;`-separated statement) retrying `counter_lock` contention / transient Keeper errors.

    Intended for large bulk loads where statements use `IF NOT EXISTS`, so retrying a partially-applied
    chunk is idempotent: already-created entities are cheap non-committing no-ops and only the remaining
    new entities enqueue.
    """
    last_err = None
    for _ in range(attempts):
        try:
            node.query(script, timeout=timeout)
            return
        except Exception as e:  # noqa: BLE001 - classify by message
            msg = str(e)
            if any(token in msg for token in CATALOG_RETRYABLE_ERRORS):
                last_err = msg
                time.sleep(sleep_time)
                continue
            raise
    raise AssertionError(
        f"run_catalog_script_with_retry exhausted {attempts} attempts\nlast error: {last_err}"
    )


def bulk_create_alternating(node_a, node_b, statements, *, chunk_size=5000):
    """
    Apply `statements` in `chunk_size` multi-statement chunks, alternating the initiator node per chunk.

    Bulk load is run serially (one chunk at a time) on purpose: the enqueue path throws `UNFINISHED`
    on `counter_lock` contention instead of queueing, so issuing large chunks concurrently would make
    almost every chunk abort mid-way and retry (a retry storm). Alternating the initiator still drives
    the ordered log from both replicas across chunks. Statements must be idempotent (`IF NOT EXISTS`).
    """
    for index in range(0, len(statements), chunk_size):
        chunk = statements[index : index + chunk_size]
        node = node_a if (index // chunk_size) % 2 == 0 else node_b
        run_catalog_script_with_retry(node, ";\n".join(chunk) + ";\n")


def wait_for_scalar(node, query, expected, *, timeout=2400, interval=2.0):
    """Poll a scalar `query` until it returns `expected` (string compare) or `timeout` elapses."""
    deadline = time.monotonic() + timeout
    last = None
    while True:
        last = node.query(query).strip()
        if last == str(expected):
            return
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"wait_for_scalar timed out: query={query!r} expected={expected!r} last={last!r}"
            )
        time.sleep(interval)


def start_two_node_cluster(test_file, encrypted, cluster_name):
    main_configs = [
        "configs/config.d/sql_catalog_replica_ddl_common.xml",
        ENCRYPTED_MAIN_CONFIGS[encrypted],
    ]
    cluster = ClickHouseCluster(test_file, name=cluster_name)
    ch1 = cluster.add_instance("ch1", main_configs=main_configs, with_zookeeper=True)
    ch2 = cluster.add_instance("ch2", main_configs=main_configs, with_zookeeper=True)
    try:
        cluster.start()
    except Exception:
        cluster.shutdown()
        raise
    return cluster, ch1, ch2


@pytest.mark.parametrize("encrypted", [pytest.param(v, id=f"encrypted_{v}") for v in ENCRYPTED])
def test_replica_ddl_keeper_backed_catalog(encrypted):
    p = f"intg_{encrypted}"
    n_alter = f"{p}_r_alter"
    n_type = f"{p}_r_type_mix"
    n_host = f"{p}_r_host"
    n_v6 = f"{p}_r_v6"
    n_fqdn = f"{p}_r_fqdn"
    n_bad = f"{p}_bad"
    n_dup = f"{p}_r_dup"
    n_missing_drop = f"{p}_r_missing"

    cluster, ch1, ch2 = start_two_node_cluster(__file__, encrypted, f"scrd_{encrypted}")

    try:
        drop_endpoint_safely(
            ch1,
            n_alter,
            n_type,
            n_host,
            n_v6,
            n_fqdn,
        )

        ch1.query(
            f"CREATE ENDPOINT {n_alter} "
            f"PROPERTIES (host = '127.0.0.1', port = 9000, secure = 0, priority = 1)"
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {n_alter} PROPERTIES (host = '127.0.0.1', port = 9000)",
            "BAD_CLUSTER_DEFINITION",
            "already exists",
        )

        ch1.query(
            f"ALTER ENDPOINT {n_alter} MODIFY PROPERTIES (secure = 1, priority = 2)"
        )

        expected_alter = f"{n_alter}\t1\t2\n"
        for node in (ch1, ch2):
            assert_eq_with_retry(
                node,
                f"SELECT name, secure, priority FROM system.endpoints "
                f"WHERE name = '{n_alter}' FORMAT TabSeparated",
                expected_alter,
            )

        assert_query_error_contains(
            ch1,
            f"DROP ENDPOINT {n_missing_drop}",
            "BAD_CLUSTER_DEFINITION",
            "does not exist",
        )
        ch1.query(f"DROP ENDPOINT IF EXISTS {n_missing_drop}")

        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {n_bad}_prop PROPERTIES (host = '127.0.0.1', port = 9000, bad_property = 1)",
            "BAD_ARGUMENTS",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {n_bad}_p0 PROPERTIES (host = '127.0.0.1', port = 0)",
            "BAD_ARGUMENTS",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {n_bad}_plarge PROPERTIES (host = '127.0.0.1', port = 70000)",
            "BAD_ARGUMENTS",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {n_bad}_htype PROPERTIES (host = 127001, port = 9000)",
            "BAD_ARGUMENTS",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {n_bad}_hempty PROPERTIES (host = '', port = 9000)",
            "BAD_ARGUMENTS",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {n_bad}_h4 PROPERTIES (host = '127..0.1', port = 9000)",
            "BAD_ARGUMENTS",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {n_bad}_h6 PROPERTIES (host = '2001:db8::123::1', port = 9000)",
            "BAD_ARGUMENTS",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {n_bad}_hhost PROPERTIES (host = '-bad.example.com', port = 9000)",
            "BAD_ARGUMENTS",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {n_dup} PROPERTIES (host = '127.0.0.1', port = 9000, secure = 0, secure = 1)",
            "BAD_ARGUMENTS",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {n_bad}_nohost PROPERTIES (port = 9000, secure = 1)",
            "BAD_ARGUMENTS",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {n_bad}_noport PROPERTIES (host = '127.0.0.1', secure = 1)",
            "BAD_ARGUMENTS",
        )

        ch1.query(
            f"CREATE ENDPOINT {n_type} PROPERTIES (host = '127.0.0.3', port = 9001, "
            f"secure = true, compression = 1, priority = 3)"
        )
        ch1.query(
            f"CREATE ENDPOINT {n_host} PROPERTIES (host = 'host1', port = 9002, secure = 0)"
        )
        ch1.query(
            f"CREATE ENDPOINT {n_v6} PROPERTIES (host = '2001:db8::123', port = 9003, secure = 0)"
        )
        ch1.query(
            f"CREATE ENDPOINT {n_fqdn} PROPERTIES (host = 'db.example.com', port = 9004, secure = 0)"
        )

        type_mix_q = (
            f"SELECT "
            f"    name, "
            f"    host = '127.0.0.3', "
            f"    port = 9001, "
            f"    secure = 1, "
            f"    compression = 1, "
            f"    priority = 3 "
            f"FROM system.endpoints WHERE name = '{n_type}' FORMAT TabSeparated"
        )
        out_type = ch1.query(type_mix_q)
        assert TSV(out_type) == TSV(f"{n_type}\t1\t1\t1\t1\t1\n")
        assert_eq_with_retry(
            ch2, type_mix_q, f"{n_type}\t1\t1\t1\t1\t1\n"
        )

        out_host = ch1.query(
            f"SELECT name, host, port FROM system.endpoints "
            f"WHERE name = '{n_host}' FORMAT TabSeparated"
        )
        assert TSV(out_host) == TSV(f"{n_host}\thost1\t9002\n")

        out_v6 = ch1.query(
            f"SELECT name, host, port FROM system.endpoints "
            f"WHERE name = '{n_v6}' FORMAT TabSeparated"
        )
        assert TSV(out_v6) == TSV(f"{n_v6}\t2001:db8::123\t9003\n")

        out_fqdn = ch1.query(
            f"SELECT name, host, port FROM system.endpoints "
            f"WHERE name = '{n_fqdn}' FORMAT TabSeparated"
        )
        assert TSV(out_fqdn) == TSV(f"{n_fqdn}\tdb.example.com\t9004\n")

        for node in (ch1, ch2):
            for sql, tsv in (
                (
                    f"SELECT name, host, port FROM system.endpoints "
                    f"WHERE name = '{n_host}' FORMAT TabSeparated",
                    f"{n_host}\thost1\t9002\n",
                ),
                (
                    f"SELECT name, host, port FROM system.endpoints "
                    f"WHERE name = '{n_fqdn}' FORMAT TabSeparated",
                    f"{n_fqdn}\tdb.example.com\t9004\n",
                ),
            ):
                assert_eq_with_retry(node, sql, tsv)

        ch1.query(f"DROP ENDPOINT IF EXISTS {n_alter}")
        ch1.query(f"DROP ENDPOINT IF EXISTS {n_type}")
        ch1.query(f"DROP ENDPOINT IF EXISTS {n_host}")
        ch1.query(f"DROP ENDPOINT IF EXISTS {n_v6}")
        ch1.query(f"DROP ENDPOINT IF EXISTS {n_fqdn}")

        c = ch1.query(
            f"SELECT count() FROM system.endpoints "
            f"WHERE name IN ('{n_alter}', '{n_type}', '{n_host}', '{n_v6}', '{n_fqdn}') "
            f"FORMAT TabSeparated"
        )
        assert c == "0\n", c
    finally:
        drop_endpoint_safely(
            ch1, n_alter, n_type, n_host, n_v6, n_fqdn, n_dup, n_missing_drop, f"{n_bad}_prop"
        )
        for suffix in (
            "prop",
            "p0",
            "plarge",
            "htype",
            "hempty",
            "h4",
            "h6",
            "hhost",
            "nohost",
            "noport",
        ):
            drop_endpoint_safely(ch1, f"{n_bad}_{suffix}")
        drop_endpoint_safely(ch1, n_dup)
        cluster.shutdown()


@pytest.mark.parametrize("encrypted", [pytest.param(v, id=f"encrypted_{v}") for v in ENCRYPTED])
def test_sql_catalog_ddl_errors(encrypted):
    """
    Negative paths for SQL-managed cluster catalog DDL: duplicates, missing entities,
    referential constraints, and cross-entity name collisions.
    """
    p = f"err_{encrypted}"
    ep = f"{p}_ep"
    ep2 = f"{p}_ep2"
    shard = f"{p}_shard"
    shard2 = f"{p}_shard2"
    cluster_name = f"{p}_cluster"
    ambig = f"{p}_ambig"
    missing = f"{p}_missing"

    cluster, ch1, _ch2 = start_two_node_cluster(__file__, encrypted, f"scrd_err_{encrypted}")

    try:
        drop_catalog_topology_safely(
            ch1,
            clusters=(cluster_name,),
            shards=(shard, shard2, ambig),
            endpoints=(ep, ep2, ambig),
        )

        # --- ENDPOINT ---
        assert_query_error_contains(
            ch1,
            f"ALTER ENDPOINT {missing} MODIFY PROPERTIES (port = 9001)",
            "BAD_CLUSTER_DEFINITION",
            "does not exist",
        )
        assert_query_error_contains(
            ch1,
            f"ALTER ENDPOINT {missing} MODIFY PROPERTIES ()",
            "BAD_ARGUMENTS",
            "requires at least one assignment",
        )

        ch1.query(
            f"CREATE ENDPOINT {ep} PROPERTIES (host = '127.0.0.1', port = 9010, secure = 0)"
        )
        ch1.query(
            f"CREATE ENDPOINT {ep2} PROPERTIES (host = '127.0.0.2', port = 9011, secure = 0)"
        )

        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {ep} PROPERTIES (host = '127.0.0.1', port = 9010)",
            "BAD_CLUSTER_DEFINITION",
            "already exists",
        )

        # --- SHARD ---
        assert_query_error_contains(
            ch1,
            f"CREATE SHARD {shard} REPLICA {missing}",
            "BAD_CLUSTER_DEFINITION",
            "does not exist",
        )

        ch1.query(f"CREATE SHARD {shard} REPLICA {ep}")

        assert_query_error_contains(
            ch1,
            f"CREATE SHARD {shard} REPLICA {ep}",
            "SHARD_ALREADY_EXISTS",
            "already exists",
        )

        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {shard} PROPERTIES (host = '127.0.0.3', port = 9012)",
            "CLUSTER_DEFINITION_NAME_AMBIGUOUS",
            "already used as SQL SHARD",
        )

        ch1.query(
            f"CREATE ENDPOINT {ambig} PROPERTIES (host = '127.0.0.4', port = 9013, secure = 0)"
        )
        assert_query_error_contains(
            ch1,
            f"CREATE SHARD {ambig} REPLICA {ep2}",
            "CLUSTER_DEFINITION_NAME_AMBIGUOUS",
            "endpoint with the same name already exists",
        )

        assert_query_error_contains(
            ch1,
            f"DROP ENDPOINT {ep}",
            "BAD_CLUSTER_DEFINITION",
            "references it",
            shard,
        )

        assert_query_error_contains(
            ch1,
            f"DROP SHARD {missing}",
            "SHARD_DOESNT_EXIST",
            "does not exist",
        )

        assert_query_error_contains(
            ch1,
            f"ALTER SHARD {missing} MODIFY PROPERTIES (weight = 2)",
            "SHARD_DOESNT_EXIST",
            "doesn't exist",
        )
        assert_query_error_contains(
            ch1,
            f"ALTER SHARD {shard} MODIFY PROPERTIES ()",
            "BAD_ARGUMENTS",
            "requires at least one shard-level assignment",
        )
        assert_query_error_contains(
            ch1,
            f"ALTER SHARD {shard} ADD REPLICA {ep}",
            "BAD_CLUSTER_DEFINITION",
            "already listed",
        )
        assert_query_error_contains(
            ch1,
            f"ALTER SHARD {shard} DROP REPLICA {ep2}",
            "BAD_CLUSTER_DEFINITION",
            "is not listed",
        )
        assert_query_error_contains(
            ch1,
            f"ALTER SHARD {shard} DROP REPLICA {ep}",
            "BAD_CLUSTER_DEFINITION",
            "Cannot DROP the last replica",
        )

        # --- CLUSTER ---
        assert_query_error_contains(
            ch1,
            f"CREATE CLUSTER {cluster_name} ({missing})",
            "SHARD_DOESNT_EXIST",
            "does not exist",
        )

        ch1.query(f"CREATE CLUSTER {cluster_name} ({shard})")

        assert_query_error_contains(
            ch1,
            f"CREATE CLUSTER {cluster_name} ({shard})",
            "CLUSTER_DEFINITION_ALREADY_EXISTS",
            "already exists",
        )

        assert_query_error_contains(
            ch1,
            f"CREATE CLUSTER {ambig} ({shard})",
            "CLUSTER_DEFINITION_NAME_AMBIGUOUS",
            "endpoint with the same name already exists",
        )

        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {cluster_name} PROPERTIES (host = '127.0.0.5', port = 9014)",
            "CLUSTER_DEFINITION_NAME_AMBIGUOUS",
            "already used as SQL CLUSTER",
        )

        ch1.query(f"CREATE SHARD {shard2} REPLICA {ep2}")
        assert_query_error_contains(
            ch1,
            f"CREATE CLUSTER {shard} ({shard2})",
            "CLUSTER_DEFINITION_NAME_AMBIGUOUS",
            "already used as SQL SHARD",
        )

        assert_query_error_contains(
            ch1,
            f"DROP SHARD {shard}",
            "SHARD_IS_REFERENCED",
            "references it",
            cluster_name,
        )

        assert_query_error_contains(
            ch1,
            f"DROP CLUSTER {missing}",
            "CLUSTER_DEFINITION_DOESNT_EXIST",
            "does not exist",
        )

        assert_query_error_contains(
            ch1,
            f"ALTER CLUSTER {missing} ADD SHARD {shard2}",
            "CLUSTER_DEFINITION_DOESNT_EXIST",
            "doesn't exist",
        )
        assert_query_error_contains(
            ch1,
            f"ALTER CLUSTER {cluster_name} ADD SHARD {shard}",
            "BAD_CLUSTER_DEFINITION",
            "already listed",
        )
        assert_query_error_contains(
            ch1,
            f"ALTER CLUSTER {cluster_name} DROP SHARD {shard2}",
            "BAD_CLUSTER_DEFINITION",
            "is not listed",
        )
        assert_query_error_contains(
            ch1,
            f"ALTER CLUSTER {cluster_name} DROP SHARD {shard}",
            "BAD_CLUSTER_DEFINITION",
            "Cannot DROP all members",
        )

        # `IF NOT EXISTS` / `IF EXISTS` must be silent no-ops on duplicates / missing entities.
        ch1.query(f"CREATE ENDPOINT IF NOT EXISTS {ep} PROPERTIES (host = '127.0.0.1', port = 9010)")
        ch1.query(f"CREATE SHARD IF NOT EXISTS {shard} REPLICA {ep}")
        ch1.query(f"CREATE CLUSTER IF NOT EXISTS {cluster_name} ({shard})")
        ch1.query(f"DROP ENDPOINT IF EXISTS {missing}")
        ch1.query(f"DROP SHARD IF EXISTS {missing}")
        ch1.query(f"DROP CLUSTER IF EXISTS {missing}")
    finally:
        drop_catalog_topology_safely(
            ch1,
            clusters=(cluster_name,),
            shards=(shard, shard2, ambig),
            endpoints=(ep, ep2, ambig),
        )
        cluster.shutdown()


@pytest.mark.parametrize("encrypted", [pytest.param(v, id=f"encrypted_{v}") for v in ENCRYPTED])
def test_sql_catalog_property_validation_errors(encrypted):
    """
    Property校验报错：semantic validation from `PropertyValidation.h` (`BAD_ARGUMENTS`).

    Mirrors stateless `04096_sql_catalog_shard_weight_uint32_range` and
    `04098_sql_catalog_property_uint64_overflow`, adapted to Keeper-backed ENDPOINT / SHARD / CLUSTER DDL.
    """
    p = f"prop_{encrypted}"
    ep = f"{p}_ep"
    ep2 = f"{p}_ep2"
    shard = f"{p}_shard"
    cluster_shard = f"{p}_cluster_shard"
    cluster_name = f"{p}_cluster"
    heavy_cluster_name = f"{p}_heavy_cluster"

    cluster, ch1, ch2 = start_two_node_cluster(__file__, encrypted, f"scrd_prop_{encrypted}")

    def assert_shard_weight(node, expected):
        assert_eq_with_retry(
            node,
            f"SELECT name, weight FROM system.shards WHERE name = '{shard}' FORMAT TabSeparated",
            f"{shard}\t{expected}\n",
        )

    try:
        drop_catalog_topology_safely(
            ch1,
            clusters=(cluster_name, heavy_cluster_name),
            shards=(shard, cluster_shard),
            endpoints=(ep, ep2),
        )

        ch1.query(
            f"CREATE ENDPOINT {ep} PROPERTIES (host = '127.0.0.1', port = 9010, secure = 0)"
        )
        ch1.query(
            f"CREATE ENDPOINT {ep2} PROPERTIES (host = '127.0.0.2', port = 9011, secure = 0)"
        )

        # --- ENDPOINT (CREATE): `port` accepts only integer literals; range 1..65535 in `validateForSQLReplica`.
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {p}_ep_str PROPERTIES (host = '127.0.0.1', port = '9000')",
            "BAD_ARGUMENTS",
            "unsigned integer literal",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {p}_ep_big PROPERTIES (host = '127.0.0.1', port = '18446744073709551617')",
            "BAD_ARGUMENTS",
            "unsigned integer literal",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {p}_ep_neg PROPERTIES (host = '127.0.0.1', port = -1)",
            "BAD_ARGUMENTS",
            "non-negative integer",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {p}_ep_flt PROPERTIES (host = '127.0.0.1', port = 9000.5)",
            "BAD_ARGUMENTS",
            "unsigned integer literal",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {p}_ep_wide PROPERTIES (host = '127.0.0.1', port = 65536)",
            "BAD_ARGUMENTS",
            "between 1 and 65535",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE ENDPOINT {p}_ep_prio PROPERTIES (host = '127.0.0.1', port = 9010, priority = 9223372036854775808)",
            "BAD_ARGUMENTS",
            "64-bit signed integer",
        )

        ch1.query(
            f"CREATE ENDPOINT {p}_ep_ok PROPERTIES (host = '127.0.0.3', port = 9012, secure = 0)"
        )
        for node in (ch1, ch2):
            assert_eq_with_retry(
                node,
                f"SELECT name, port FROM system.endpoints WHERE name = '{p}_ep_ok' FORMAT TabSeparated",
                f"{p}_ep_ok\t9012\n",
            )

        # --- ENDPOINT (ALTER): patch path uses `narrowPortToUInt16` / `narrowPriorityToInt64`.
        assert_query_error_contains(
            ch1,
            f"ALTER ENDPOINT {ep} MODIFY PROPERTIES (port = 65536)",
            "BAD_ARGUMENTS",
            "16-bit unsigned integer",
        )
        assert_query_error_contains(
            ch1,
            f"ALTER ENDPOINT {ep} MODIFY PROPERTIES (port = 0)",
            "BAD_ARGUMENTS",
            "positive `port`",
        )
        assert_query_error_contains(
            ch1,
            f"ALTER ENDPOINT {ep} MODIFY PROPERTIES (priority = 9223372036854775808)",
            "BAD_ARGUMENTS",
            "64-bit signed integer",
        )

        # --- SHARD (CREATE): `weight` UInt32 range; integer literals only.
        assert_query_error_contains(
            ch1,
            f"CREATE SHARD {shard} REPLICA {ep} PROPERTIES (weight = 4294967296)",
            "BAD_ARGUMENTS",
            "32-bit unsigned integer",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE SHARD {shard} REPLICA {ep} PROPERTIES (weight = 4294967297)",
            "BAD_ARGUMENTS",
            "32-bit unsigned integer",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE SHARD {shard} REPLICA {ep} PROPERTIES (weight = 0)",
            "BAD_ARGUMENTS",
            "greater than zero",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE SHARD {shard} REPLICA {ep} PROPERTIES (weight = -1)",
            "BAD_ARGUMENTS",
            "non-negative integer",
        )

        assert_query_error_contains(
            ch1,
            f"CREATE SHARD {shard} REPLICA {ep} PROPERTIES (weight = '7')",
            "BAD_ARGUMENTS",
            "unsigned integer literal",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE SHARD {shard} REPLICA {ep} PROPERTIES (weight = 7.0)",
            "BAD_ARGUMENTS",
            "unsigned integer literal",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE SHARD {shard} REPLICA {ep} PROPERTIES (internal_replication = 'maybe')",
            "BAD_ARGUMENTS",
            "boolean true or false",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE SHARD {shard} REPLICA {ep} PROPERTIES (host = '127.0.0.1')",
            "BAD_ARGUMENTS",
            "shard-level PROPERTIES",
        )

        ch1.query(
            f"CREATE SHARD {shard} REPLICA {ep} PROPERTIES (weight = 4294967295)"
        )
        for node in (ch1, ch2):
            assert_shard_weight(node, 4294967295)

        assert_query_error_contains(
            ch1,
            f"CREATE CLUSTER {heavy_cluster_name} ({shard})",
            "BAD_ARGUMENTS",
            "total shard weight",
        )

        ch1.query(f"CREATE SHARD {cluster_shard} REPLICA {ep2} PROPERTIES (weight = 1)")

        # --- CLUSTER (CREATE): cluster-level keys only; `secret` must be a string.
        # Use a separate lightweight shard: materialized `Cluster` expands `weight` into `slot_to_shard`.
        assert_query_error_contains(
            ch1,
            f"CREATE CLUSTER {cluster_name} ({cluster_shard}) PROPERTIES (bad_key = 1)",
            "BAD_ARGUMENTS",
            "cluster-level PROPERTIES",
        )
        assert_query_error_contains(
            ch1,
            f"CREATE CLUSTER {cluster_name} ({cluster_shard}) PROPERTIES (secret = 123)",
            "BAD_ARGUMENTS",
            "must be a string",
        )

        ch1.query(
            f"CREATE CLUSTER {cluster_name} ({cluster_shard}) PROPERTIES (allow_distributed_ddl_queries = 0)"
        )
        for node in (ch1, ch2):
            assert_eq_with_retry(
                node,
                f"SHOW CREATE CLUSTER {cluster_name}",
                f"CREATE CLUSTER {cluster_name} ({cluster_shard}) PROPERTIES (allow_distributed_ddl_queries = false)\n",
            )

        # --- SHARD (ALTER MODIFY): same `parseWeightValue` rules; failed ALTER must not mutate catalog.
        for weight_sql in ("4294967296", "4294967297", "0", "-1", "'8'", "8.0"):
            assert_query_error_contains(
                ch1,
                f"ALTER SHARD {shard} MODIFY PROPERTIES (weight = {weight_sql})",
                "BAD_ARGUMENTS",
            )
        for node in (ch1, ch2):
            assert_shard_weight(node, 4294967295)

        assert_query_error_contains(
            ch1,
            f"ALTER SHARD {shard} MODIFY PROPERTIES (internal_replication = 2)",
            "BAD_ARGUMENTS",
            "boolean true or false",
        )
        for node in (ch1, ch2):
            assert_shard_weight(node, 4294967295)

        ch1.query(f"ALTER SHARD {shard} MODIFY PROPERTIES (weight = 7)")
        for node in (ch1, ch2):
            assert_shard_weight(node, 7)

        # --- SHARD (ALTER REPLACE ... MODIFY PROPERTIES): optional tail uses the same weight checks.
        assert_query_error_contains(
            ch1,
            f"ALTER SHARD {shard} REPLACE {ep} TO {ep2} MODIFY PROPERTIES (weight = 4294967296)",
            "BAD_ARGUMENTS",
            "32-bit unsigned integer",
        )
        assert_query_error_contains(
            ch1,
            f"ALTER SHARD {shard} REPLACE {ep} TO {ep2} MODIFY PROPERTIES (weight = 0)",
            "BAD_ARGUMENTS",
            "greater than zero",
        )
        for node in (ch1, ch2):
            assert_shard_weight(node, 7)
            assert_eq_with_retry(
                node,
                f"SELECT length(endpoints), endpoints[1] FROM system.shards WHERE name = '{shard}' FORMAT TabSeparated",
                f"1\t{ep}\n",
            )

        ch1.query(
            f"ALTER SHARD {shard} REPLACE {ep} TO {ep2} MODIFY PROPERTIES (weight = 4294967295)"
        )
        for node in (ch1, ch2):
            assert_shard_weight(node, 4294967295)
            assert_eq_with_retry(
                node,
                f"SELECT length(endpoints), endpoints[1] FROM system.shards WHERE name = '{shard}' FORMAT TabSeparated",
                f"1\t{ep2}\n",
            )

        ch1.query(f"DROP ENDPOINT IF EXISTS {p}_ep_ok")
    finally:
        drop_catalog_topology_safely(
            ch1,
            clusters=(cluster_name, heavy_cluster_name),
            shards=(shard, cluster_shard),
            endpoints=(ep, ep2, f"{p}_ep_ok"),
        )
        for suffix in ("str", "big", "neg", "flt", "wide", "prio"):
            drop_endpoint_safely(ch1, f"{p}_ep_{suffix}")
        cluster.shutdown()
