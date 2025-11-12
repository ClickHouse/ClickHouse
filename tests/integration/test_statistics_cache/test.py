import uuid
import threading
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance("ch1", stay_alive=True)
r1  = cluster.add_instance("r1", with_zookeeper=True, stay_alive=True)
r2  = cluster.add_instance("r2", with_zookeeper=True, stay_alive=True)

TEST_DB = f"db_sc_{uuid.uuid4().hex[:6]}"
SET_PREFIX = f"""\
USE {TEST_DB};
SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;
SET mutations_sync = 2;
SET log_queries = 1;
SET log_query_settings = 1;
"""

ROWS_SMALL  = 200_000
ROWS_MEDIUM = 300_000
ROWS_JOIN   = 120_000

def _query(node, sql):
    return node.query(SET_PREFIX + "\n" + sql)

def _query_retry(node, sql, retry_count=10, sleep_time=0.2):
    return node.query_with_retry(
        SET_PREFIX + "\n" + sql,
        retry_count=retry_count,
        sleep_time=sleep_time,
    )

def _flush_logs(node):
    _query(node, "SYSTEM FLUSH LOGS query_log")

def _loaded_stats_us(node, tag):
    _flush_logs(node)
    v = _query(node, f"""
        SELECT toInt64(coalesce(max(ProfileEvents['LoadedStatisticsMicroseconds']), 0))
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND log_comment = '{tag}'
        FORMAT TabSeparated
    """).strip()
    return int(v or "0")

def _assert_hit(node, tag):
    assert _loaded_stats_us(node, tag) == 0

def _assert_load(node, tag):
    assert _loaded_stats_us(node, tag) > 0

def _wait_hit(node, tag, query_sql, retry_count=20, sleep_time=0.2):
    return node.query_with_retry(
        SET_PREFIX + "\n" + query_sql,
        retry_count=retry_count,
        sleep_time=sleep_time,
        check_callback=lambda _: (_loaded_stats_us(node, tag) == 0),
    )

def _create_tbl(node, name, interval):
    _query_retry(node, f"DROP TABLE IF EXISTS {name} SYNC")
    _query_retry(node, f"""
        CREATE TABLE {name} (k UInt32, v Float64)
        ENGINE=MergeTree
        ORDER BY k
        SETTINGS refresh_statistics_interval = {interval}
    """)
    _query(node, f"INSERT INTO {name} SELECT number, toFloat64(rand())/4294967296.0 FROM numbers({ROWS_SMALL})")
    _query_retry(node, f"ALTER TABLE {name} ADD STATISTICS v TYPE TDigest")
    _query_retry(node, f"ALTER TABLE {name} MATERIALIZE STATISTICS ALL")

def _create_src_tbl(node, name, interval):
    _query_retry(node, f"DROP TABLE IF EXISTS {name} SYNC")
    _query_retry(node, f"""
        CREATE TABLE {name} (k UInt32, v Float64, p Date)
        ENGINE=MergeTree PARTITION BY toYYYYMM(p) ORDER BY k
        SETTINGS refresh_statistics_interval = {interval}
    """)
    _query(node, f"""
        INSERT INTO {name}
        SELECT number,
               toFloat64(rand())/4294967296.0,
               toDate('2024-10-01') + (number%10)
        FROM numbers({ROWS_MEDIUM})
    """)
    _query_retry(node, f"ALTER TABLE {name} ADD STATISTICS v TYPE TDigest")
    _query_retry(node, f"ALTER TABLE {name} MATERIALIZE STATISTICS ALL")

def _create_join(node, prefix, interval):
    l, p = f"{prefix}_l", f"{prefix}_p"
    _query_retry(node, f"DROP TABLE IF EXISTS {l} SYNC")
    _query_retry(node, f"DROP TABLE IF EXISTS {p} SYNC")
    _query_retry(node, f"""
        CREATE TABLE {l} (
          l_partkey UInt32, l_extendedprice Float64, l_discount Float64, l_shipdate Date
        ) ENGINE=MergeTree ORDER BY l_partkey
        SETTINGS refresh_statistics_interval = {interval}
    """)
    _query_retry(node, f"""
        CREATE TABLE {p} (
          p_partkey UInt32, p_type LowCardinality(String)
        ) ENGINE=MergeTree ORDER BY p_partkey
        SETTINGS refresh_statistics_interval = {interval}
    """)
    _query(node, f"""
        INSERT INTO {l}
        SELECT number, 1.0, 0.05, toDate('2024-09-01') + (number%30)
        FROM numbers({ROWS_JOIN})
    """)
    _query(node, f"INSERT INTO {p} SELECT number, if(number%5=0,'PROMO','OTHER') FROM numbers({ROWS_JOIN})")
    _query_retry(node, f"ALTER TABLE {l} ADD STATISTICS l_partkey TYPE Uniq")
    _query_retry(node, f"ALTER TABLE {p} ADD STATISTICS p_partkey TYPE Uniq")
    _query_retry(node, f"ALTER TABLE {l} MATERIALIZE STATISTICS ALL")
    _query_retry(node, f"ALTER TABLE {p} MATERIALIZE STATISTICS ALL")
    return l, p

def _create_rep(interval):
    table = "rep_sc"
    for n in (r1, r2):
        _query_retry(n, f"CREATE DATABASE IF NOT EXISTS {TEST_DB}")
        _query_retry(n, f"DROP TABLE IF EXISTS {table} SYNC")

    _query_retry(r1, f"""
        CREATE TABLE {table} (id UInt32, v Float64)
        ENGINE=ReplicatedMergeTree('/clickhouse/tests/{TEST_DB}/{table}', 'r1')
        ORDER BY id
        SETTINGS refresh_statistics_interval = {interval}
    """)
    _query_retry(r2, f"""
        CREATE TABLE {table} (id UInt32, v Float64)
        ENGINE=ReplicatedMergeTree('/clickhouse/tests/{TEST_DB}/{table}', 'r2')
        ORDER BY id
        SETTINGS refresh_statistics_interval = {interval}
    """)

    _query(r1, f"INSERT INTO {table} SELECT number, toFloat64(rand())/4294967296.0 FROM numbers({ROWS_SMALL})")
    _query_retry(r2, f"SYSTEM SYNC REPLICA {table}", retry_count=60, sleep_time=0.2)

    _query_retry(r1, f"ALTER TABLE {table} ADD STATISTICS v TYPE TDigest")
    _query_retry(r1, f"ALTER TABLE {table} MATERIALIZE STATISTICS ALL")

    _query_retry(r1, f"SYSTEM SYNC REPLICA {table}", retry_count=60, sleep_time=0.2)
    _query_retry(r2, f"SYSTEM SYNC REPLICA {table}", retry_count=60, sleep_time=0.2)
    return table

@pytest.fixture(scope="module", autouse=True)
def _started_cluster():
    cluster.start()
    ch1.query(f"CREATE DATABASE IF NOT EXISTS {TEST_DB}")
    r1.query(f"CREATE DATABASE IF NOT EXISTS {TEST_DB}")
    r2.query(f"CREATE DATABASE IF NOT EXISTS {TEST_DB}")
    try:
        yield
    finally:
        for n in (ch1, r1, r2):
            n.query(f"DROP DATABASE IF EXISTS {TEST_DB} SYNC")
        cluster.shutdown()

def test_load_then_hit_single_node():
    _create_tbl(ch1, "c_tbl", 1)
    _query(ch1, "SELECT count() FROM c_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=0, log_comment='load' FORMAT Null")
    _assert_load(ch1, "load")
    _wait_hit(
        ch1, "hit",
        "SELECT count() FROM c_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='hit' FORMAT Null"
    )

def test_session_toggle_does_not_corrupt_cache():
    _create_tbl(ch1, "toggle_tbl", 1)
    _wait_hit(
        ch1, "toggle-hit1",
        "SELECT count() FROM toggle_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='toggle-hit1' FORMAT Null"
    )
    _query(ch1, "SELECT count() FROM toggle_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=0, log_comment='toggle-load' FORMAT Null")
    _assert_load(ch1, "toggle-load")
    _wait_hit(
        ch1, "toggle-hit2",
        "SELECT count() FROM toggle_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='toggle-hit2' FORMAT Null"
    )

def test_interval_zero_disables_cache():
    _create_tbl(ch1, "z_tbl", 1)
    _wait_hit(
        ch1, "z-warm",
        "SELECT count() FROM z_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='z-warm' FORMAT Null"
    )
    _query_retry(ch1, "ALTER TABLE z_tbl MODIFY SETTING refresh_statistics_interval = 0")
    _query_retry(ch1, "DETACH TABLE z_tbl; ATTACH TABLE z_tbl")
    _query(ch1, "SELECT count() FROM z_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='z-load1' FORMAT Null")
    _assert_load(ch1, "z-load1")
    _query(ch1, "SELECT count() FROM z_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='z-load2' FORMAT Null")
    _assert_load(ch1, "z-load2")

def test_staleness_after_inserts_stays_hit():
    _create_tbl(ch1, "ins_tbl", 1)
    _wait_hit(
        ch1, "ins-hit1",
        "SELECT count() FROM ins_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='ins-hit1' FORMAT Null"
    )
    _query(ch1, f"INSERT INTO ins_tbl SELECT number+{ROWS_SMALL}, toFloat64(rand())/4294967296.0 FROM numbers({ROWS_SMALL})")
    _wait_hit(
        ch1, "ins-hit2",
        "SELECT count() FROM ins_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='ins-hit2' FORMAT Null"
    )

def test_mutation_optimize_replace_drop_keep_hit():
    _create_src_tbl(ch1, "mut_tbl", 1)
    _wait_hit(
        ch1, "mut-warm",
        "SELECT count() FROM mut_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='mut-warm' FORMAT Null"
    )
    _query_retry(ch1, "ALTER TABLE mut_tbl UPDATE v = v + 0.001 WHERE k%2=0")
    _wait_hit(
        ch1, "mut-after-update",
        "SELECT count() FROM mut_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='mut-after-update' FORMAT Null"
    )
    _query_retry(ch1, "OPTIMIZE TABLE mut_tbl FINAL")
    _wait_hit(
        ch1, "mut-after-opt",
        "SELECT count() FROM mut_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='mut-after-opt' FORMAT Null"
    )
    _create_src_tbl(ch1, "mut_src", 1)
    _query_retry(ch1, "ALTER TABLE mut_tbl REPLACE PARTITION 202410 FROM mut_src")
    _wait_hit(
        ch1, "mut-after-replace",
        "SELECT count() FROM mut_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='mut-after-replace' FORMAT Null"
    )
    part = _query(ch1, "SELECT name FROM system.parts WHERE database=currentDatabase() AND table='mut_tbl' AND active LIMIT 1 FORMAT TabSeparated").strip()
    _query_retry(ch1, f"ALTER TABLE mut_tbl DROP PART '{part}'")
    _wait_hit(
        ch1, "mut-after-drop",
        "SELECT count() FROM mut_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='mut-after-drop' FORMAT Null"
    )

def test_join_load_then_hit_and_parallel_readers():
    l, p = _create_join(ch1, "jn", 1)
    _query(ch1, f"""
        SELECT count()
        FROM {l} AS l, {p} AS p
        WHERE l.l_partkey=p.p_partkey AND p.p_type='PROMO'
        SETTINGS use_statistics_cache=0, query_plan_optimize_join_order_limit=10, log_comment='join-load'
        FORMAT Null
    """)
    _assert_load(ch1, "join-load")
    _wait_hit(
        ch1, "join-hit", f"""
        SELECT count()
        FROM {l} AS l, {p} AS p
        WHERE l.l_partkey=p.p_partkey AND p.p_type='PROMO'
        SETTINGS use_statistics_cache=1, query_plan_optimize_join_order_limit=10, log_comment='join-hit'
        FORMAT Null
    """)
    errs = []

    def _reader(i):
        tag = f"join-r-{i}"
        try:
            _query(ch1, f"""
                SELECT count()
                FROM {l} AS l, {p} AS p
                WHERE l.l_partkey=p.p_partkey AND p.p_type='PROMO'
                SETTINGS use_statistics_cache=1, query_plan_optimize_join_order_limit=10, log_comment='{tag}'
                FORMAT Null
            """)
            if _loaded_stats_us(ch1, tag) != 0:
                errs.append(f"loaded:{i}")
        except Exception as ex:
            errs.append(f"ex:{i}:{ex}")

    threads = [threading.Thread(target=_reader, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errs, f"errors: {errs}"

def test_alter_interval_requires_detach_attach():
    _create_tbl(ch1, "alt_tbl", 0)
    _query(ch1, "SELECT count() FROM alt_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='alt-pre' FORMAT Null")
    _assert_load(ch1, "alt-pre")
    _query_retry(ch1, "ALTER TABLE alt_tbl MODIFY SETTING refresh_statistics_interval = 1")
    _query(ch1, "SELECT count() FROM alt_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='alt-now' FORMAT Null")
    _assert_load(ch1, "alt-now")
    _query_retry(ch1, "DETACH TABLE alt_tbl; ATTACH TABLE alt_tbl")
    _wait_hit(
        ch1, "alt-post",
        "SELECT count() FROM alt_tbl WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='alt-post' FORMAT Null"
    )

def test_types_smoke_and_nullable():
    _query_retry(ch1, "DROP TABLE IF EXISTS t_cm SYNC")
    _query_retry(ch1, "CREATE TABLE t_cm (k UInt32, cat String) ENGINE=MergeTree ORDER BY k SETTINGS refresh_statistics_interval=1")
    _query(ch1, f"INSERT INTO t_cm SELECT number, if(number%4=0,'PROMO',concat('X',toString(number%1000))) FROM numbers({ROWS_SMALL})")
    _query_retry(ch1, "ALTER TABLE t_cm ADD STATISTICS cat TYPE CountMin")
    _query_retry(ch1, "ALTER TABLE t_cm MATERIALIZE STATISTICS ALL")
    _wait_hit(
        ch1, "t-cm",
        "SELECT count() FROM t_cm WHERE cat='PROMO' AND k>=0 SETTINGS use_statistics_cache=1, log_comment='t-cm' FORMAT Null"
    )

    _query_retry(ch1, "DROP TABLE IF EXISTS t_mm SYNC")
    _query_retry(ch1, "CREATE TABLE t_mm (k UInt32, x UInt32) ENGINE=MergeTree ORDER BY k SETTINGS refresh_statistics_interval=1")
    _query(ch1, f"INSERT INTO t_mm SELECT number, number%1000000 FROM numbers({ROWS_MEDIUM})")
    _query_retry(ch1, "ALTER TABLE t_mm ADD STATISTICS x TYPE MinMax")
    _query_retry(ch1, "ALTER TABLE t_mm MATERIALIZE STATISTICS ALL")
    _wait_hit(
        ch1, "t-mm",
        "SELECT count() FROM t_mm WHERE x BETWEEN 900000 AND 950000 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='t-mm' FORMAT Null"
    )

    _create_tbl(ch1, "t_tdg", 1)
    _wait_hit(
        ch1, "t-tdg",
        "SELECT count() FROM t_tdg WHERE v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='t-tdg' FORMAT Null"
    )

    _query_retry(ch1, "DROP TABLE IF EXISTS t_tdg_null SYNC")
    _query_retry(ch1, "CREATE TABLE t_tdg_null (k UInt32, v Nullable(Float64)) ENGINE=MergeTree ORDER BY k SETTINGS refresh_statistics_interval=1")
    _query(ch1, f"INSERT INTO t_tdg_null SELECT number, multiIf(number%10=0,NULL,toFloat64(rand())/4294967296.0) FROM numbers({ROWS_SMALL})")
    _query_retry(ch1, "ALTER TABLE t_tdg_null ADD STATISTICS v TYPE TDigest")
    _query_retry(ch1, "ALTER TABLE t_tdg_null MATERIALIZE STATISTICS ALL")
    _wait_hit(
        ch1, "t-tdg-null",
        "SELECT count() FROM t_tdg_null WHERE v IS NOT NULL AND v>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='t-tdg-null' FORMAT Null"
    )

def test_countmin_lowcardinality_supported_load_then_hit():
    _query_retry(ch1, "DROP TABLE IF EXISTS t_cm_lc SYNC")
    _query_retry(ch1, "CREATE TABLE t_cm_lc (k UInt32, cat LowCardinality(String)) ENGINE=MergeTree ORDER BY k SETTINGS refresh_statistics_interval=1")
    _query(ch1, f"INSERT INTO t_cm_lc SELECT number, if(number%4=0,'PROMO',concat('X',toString(number%1000))) FROM numbers({ROWS_SMALL})")
    _query_retry(ch1, "ALTER TABLE t_cm_lc ADD STATISTICS cat TYPE CountMin")
    _query_retry(ch1, "ALTER TABLE t_cm_lc MATERIALIZE STATISTICS ALL")
    _query(ch1, "SELECT count() FROM t_cm_lc WHERE cat='PROMO' AND k>=0 SETTINGS use_statistics_cache=0, log_comment='cm-lc-load' FORMAT Null")
    _assert_load(ch1, "cm-lc-load")
    _wait_hit(
        ch1, "cm-lc-hit",
        "SELECT count() FROM t_cm_lc WHERE cat='PROMO' AND k>=0 SETTINGS use_statistics_cache=1, log_comment='cm-lc-hit' FORMAT Null"
    )

def test_drop_statistics_means_no_load_and_bypass_still_loads():
    _create_tbl(ch1, "drop_tbl", 1)
    _query_retry(ch1, "ALTER TABLE drop_tbl DROP STATISTICS v")

    _query(ch1,
           "SELECT count() FROM drop_tbl WHERE v>0.99 AND k>=0 "
           "SETTINGS use_statistics_cache=0, log_comment='drop-bypass' FORMAT Null")
    _assert_load(ch1, "drop-bypass")

    _wait_hit(
        ch1, "drop-hit",
        "SELECT count() FROM drop_tbl WHERE v>0.99 AND k>=0 "
        "SETTINGS use_statistics_cache=1, log_comment='drop-hit' FORMAT Null"
    )

def test_per_replica_cache_and_restart_needed():
    table = _create_rep(0)
    _query(r1, f"SELECT count() FROM {table} WHERE v>0.99 AND id>=0 SETTINGS use_statistics_cache=1, log_comment='rep-r1-pre' FORMAT Null")
    _assert_load(r1, "rep-r1-pre")
    _query(r2, f"SELECT count() FROM {table} WHERE v>0.99 AND id>=0 SETTINGS use_statistics_cache=1, log_comment='rep-r2-pre' FORMAT Null")
    _assert_load(r2, "rep-r2-pre")

    for n in (r1, r2):
        _query_retry(n, f"ALTER TABLE {table} MODIFY SETTING refresh_statistics_interval = 1")

    _query(r1, f"SELECT count() FROM {table} WHERE v>0.99 AND id>=0 SETTINGS use_statistics_cache=1, log_comment='rep-now' FORMAT Null")
    _assert_load(r1, "rep-now")

    _query_retry(r1, f"SYSTEM RESTART REPLICA {table}")
    _query_retry(r2, f"SYSTEM RESTART REPLICA {table}")

    _wait_hit(
        r1, "rep-post-r1",
        f"SELECT count() FROM {table} WHERE v>0.99 AND id>=0 SETTINGS use_statistics_cache=1, log_comment='rep-post-r1' FORMAT Null"
    )
    _wait_hit(
        r2, "rep-post-r2",
        f"SELECT count() FROM {table} WHERE v>0.99 AND id>=0 SETTINGS use_statistics_cache=1, log_comment='rep-post-r2' FORMAT Null"
    )

def test_replication_inserts_keep_hit():
    table = _create_rep(1)
    _wait_hit(
        r1, "rep-warm",
        f"SELECT count() FROM {table} WHERE v>0.99 AND id>=0 SETTINGS use_statistics_cache=1, log_comment='rep-warm' FORMAT Null"
    )
    _query(r2, f"INSERT INTO {table} SELECT number+{ROWS_SMALL}, toFloat64(rand())/4294967296.0 FROM numbers(100000)")
    _wait_hit(
        r1, "rep-post",
        f"SELECT count() FROM {table} WHERE v>0.99 AND id>=0 SETTINGS use_statistics_cache=1, log_comment='rep-post' FORMAT Null"
    )

def test_auto_statistics_types_load_then_hit():
    _query_retry(ch1, "DROP TABLE IF EXISTS auto_tbl SYNC")
    _query_retry(ch1, """
        CREATE TABLE auto_tbl
        (
          k UInt32,
          val Float64,
          cat String,
          x UInt32
        )
        ENGINE=MergeTree
        ORDER BY k
        SETTINGS refresh_statistics_interval=1,
                 auto_statistics_types='tdigest,countmin,minmax,uniq'
    """)
    _query(ch1, f"""
        INSERT INTO auto_tbl
        SELECT number,
               toFloat64(rand())/4294967296.0,
               if(number%7=0,'PROMO', concat('G',toString(number%500))),
               number%1000000
        FROM numbers({ROWS_SMALL})
    """)
    _query_retry(ch1, "ALTER TABLE auto_tbl MATERIALIZE STATISTICS ALL")

    _query(ch1, "SELECT count() FROM auto_tbl WHERE val>0.99 AND k>=0 SETTINGS use_statistics_cache=0, log_comment='auto-td-load' FORMAT Null")
    _assert_load(ch1, "auto-td-load")
    _wait_hit(
        ch1, "auto-td-hit",
        "SELECT count() FROM auto_tbl WHERE val>0.99 AND k>=0 SETTINGS use_statistics_cache=1, log_comment='auto-td-hit' FORMAT Null"
    )

    _query(ch1, "SELECT count() FROM auto_tbl WHERE cat='PROMO' AND k>=0 SETTINGS use_statistics_cache=0, log_comment='auto-cm-load' FORMAT Null")
    _assert_load(ch1, "auto-cm-load")
    _wait_hit(
        ch1, "auto-cm-hit",
        "SELECT count() FROM auto_tbl WHERE cat='PROMO' AND k>=0 SETTINGS use_statistics_cache=1, log_comment='auto-cm-hit' FORMAT Null"
    )
