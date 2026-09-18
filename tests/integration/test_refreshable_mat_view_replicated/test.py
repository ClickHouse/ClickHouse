import logging
import threading
import time
from datetime import datetime
from typing import Optional

import pytest
from jinja2 import Environment, Template

import helpers.client
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, wait_condition

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node1_1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": 1, "replica": 1},
)
node2 = cluster.add_instance(
    "node1_2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": 1, "replica": 2},
)

nodes = [node, node2]


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


"""
### TESTS
+1. Append mode
2. Restart node and wait for restore
+3. Simple functional testing: all values in refresh result correct (two and more rmv)
+4. Combinations of intervals
+5. Two (and more) rmv from single to single [APPEND]
+6. ALTER rmv ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
+7. RMV without tgt table (automatic table) (check APPEND)

+8 DROP rmv
+9 CREATE - DROP - CREATE - ALTER
11. Long queries over refresh time (check settings)
13. incorrect intervals (interval 1 sec, offset 1 minute)
    - OFFSET less than the period. 'EVERY 1 MONTH OFFSET 5 WEEK'
    - cases below
+14. ALTER on cluster

15. write to distributed with / without APPEND
17. Not existent TO table (ON CLUSTER)
18. Not existent FROM table (ON CLUSTER)
19. Not existent BOTH tables (ON CLUSTER)
+20. retry failed
21. overflow with wait test
22. ON CLUSTER

+SYSTEM STOP|START|REFRESH|CANCEL VIEW
+SYSTEM WAIT VIEW [db.]name

"""


def j2_template(
    string: str,
    globals: Optional[dict] = None,
    filters: Optional[dict] = None,
    tests: Optional[dict] = None,
) -> Template:
    def uppercase(value: str):
        return value.upper()

    def lowercase(value: str):
        return value.lower()

    def format_settings(items: dict):
        return ", ".join([f"{k}={v}" for k, v in items.items()])

    # Create a custom environment and add the functions
    env = Environment(
        trim_blocks=False, lstrip_blocks=True, keep_trailing_newline=False
    )
    env.globals["uppercase"] = uppercase
    env.globals["lowercase"] = lowercase
    env.filters["format_settings"] = format_settings

    if filters:
        env.filters.update(filters)
    if globals:
        env.globals.update(globals)
    if tests:
        env.tests.update(tests)

    return env.from_string(string)


def assert_same_values(lst: list):
    if not isinstance(lst, list):
        lst = list(lst)
    assert all(x == lst[0] for x in lst)


RMV_TEMPLATE = """{{ refresh_interval }}
{% if depends_on %}DEPENDS ON {{ depends_on|join(', ') }}{% endif %}
{% if settings %}SETTINGS {{ settings|format_settings }}{% endif %}
{% if with_append %}APPEND{% endif %}
{% if to_clause %}TO {{ to_clause }}{% endif %}
{% if table_clause %}{{ table_clause }}{% endif %}
{% if empty %}EMPTY{% endif %}
{% if select_query %} AS {{ select_query }}{% endif %}
"""

CREATE_RMV = j2_template(
    """CREATE MATERIALIZED VIEW
{% if if_not_exists %}IF NOT EXISTS{% endif %}
{% if db %}{{db}}.{% endif %}{{ table_name }}
{% if on_cluster %}ON CLUSTER {{ on_cluster }}{% endif %}
REFRESH
"""
    + RMV_TEMPLATE
)

ALTER_RMV = j2_template(
    """ALTER TABLE
{% if db %}{{db}}.{% endif %}{{ table_name }}
{% if on_cluster %}ON CLUSTER {{ on_cluster }}{% endif %}
MODIFY REFRESH
"""
    + RMV_TEMPLATE
)


@pytest.fixture(scope="module", autouse=True)
def module_setup_tables(started_cluster):

    # default is Atomic by default
    node.query("DROP DATABASE IF EXISTS default ON CLUSTER default SYNC")
    node.query(
        "CREATE DATABASE default ON CLUSTER default ENGINE=Replicated('/clickhouse/default/','{shard}','{replica}')"
    )

    assert (
        node.query(
            "SELECT engine FROM clusterAllReplicas(default, system.databases) where name='default'"
        )
        == "Replicated\nReplicated\n"
    )

    node.query("DROP DATABASE IF EXISTS test_db ON CLUSTER default SYNC")
    node.query(
        "CREATE DATABASE test_db ON CLUSTER default ENGINE=Replicated('/clickhouse/test_db/','{shard}','{replica}')"
    )

    assert (
        node.query(
            "SELECT engine FROM clusterAllReplicas(default, system.databases) where name='test_db'"
        )
        == "Replicated\nReplicated\n"
    )

    node.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS test_db.test_rmv")
    node.query("DROP TABLE IF EXISTS src1 ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS src2 ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS tgt1 ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS tgt2 ON CLUSTER default")

    node.query(
        "CREATE TABLE src1 ON CLUSTER default (a DateTime, b UInt64) ENGINE = ReplicatedMergeTree() ORDER BY tuple()"
    )
    node.query(
        "CREATE TABLE src2 ON CLUSTER default (a DateTime, b UInt64) ENGINE = ReplicatedMergeTree() ORDER BY tuple()"
    )
    node.query(
        "CREATE TABLE tgt1 ON CLUSTER default (a DateTime, b UInt64) ENGINE = ReplicatedMergeTree() ORDER BY tuple()"
    )
    node.query(
        "CREATE TABLE tgt2 ON CLUSTER default (a DateTime, b UInt64) ENGINE = ReplicatedMergeTree() ORDER BY tuple()"
    )
    node.query(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS dummy_rmv ON CLUSTER default "
        "REFRESH EVERY 10 HOUR ENGINE = ReplicatedMergeTree() ORDER BY tuple() EMPTY AS select number as x from numbers(1)"
    )


@pytest.fixture(scope="function")
def fn_setup_tables():
    node.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS test_db.test_rmv")
    node.query("DROP TABLE IF EXISTS src1 ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS tgt1 ON CLUSTER default")

    node.query(
        "CREATE TABLE tgt1 ON CLUSTER default (a DateTime, b UInt64) "
        "ENGINE = ReplicatedMergeTree ORDER BY tuple()"
    )

    node.query(
        "CREATE TABLE src1 ON CLUSTER default (a DateTime, b UInt64) "
        "ENGINE = ReplicatedMergeTree ORDER BY tuple()"
    )
    node.query("INSERT INTO src1 VALUES ('2020-01-01', 1), ('2020-01-02', 2)")

    yield

    node.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS test_db.test_rmv")


def opposite_minutes():
    return (60 - datetime.now().minute) % 60


@pytest.mark.parametrize(
    "select_query",
    [
        "SELECT now() as a, number as b FROM numbers(2) SETTINGS insert_deduplicate=0",
        "SELECT now() as a, b as b FROM src1 SETTINGS insert_deduplicate=0",
    ],
)
@pytest.mark.parametrize(
    "with_append",
    [True, False],
)
@pytest.mark.parametrize(
    "empty",
    [True, False],
)
def test_append(
    module_setup_tables,
    fn_setup_tables,
    select_query,
    with_append,
    empty,
):
    create_sql = CREATE_RMV.render(
        table_name="test_rmv",
        refresh_interval=f"EVERY 1 HOUR OFFSET {opposite_minutes()} MINUTE",
        to_clause="tgt1",
        select_query=select_query,
        with_append=with_append,
        on_cluster="default",
        empty=empty,
    )
    node.query(create_sql)
    rmv = get_rmv_info(node, "test_rmv", wait_status="Scheduled")
    assert rmv["exception"] is None

    records = node.query("SELECT count() FROM test_rmv")

    if empty:
        assert records == "0\n"
    else:
        assert records == "2\n"

    node.query(f"SYSTEM TEST VIEW test_rmv SET FAKE TIME '{rmv['next_refresh_time']}'")

    rmv2 = get_rmv_info(node, "test_rmv", wait_status="Scheduled")

    assert rmv2["exception"] is None

    expect = "2\n"
    if with_append and not empty:
        expect = "4\n"

    records = node.query_with_retry(
        "SELECT count() FROM test_rmv", check_callback=lambda x: x == expect
    )
    assert records == expect


@pytest.mark.parametrize("with_append", [True, False])
@pytest.mark.parametrize("depends_on", [None, ["default.dummy_rmv"]])
@pytest.mark.parametrize("empty", [True, False])
@pytest.mark.parametrize("database_name", ["test_db"])
@pytest.mark.parametrize(
    "settings",
    [
        {},
        {
            "refresh_retries": "10",
            "refresh_retry_initial_backoff_ms": "10",
            "refresh_retry_max_backoff_ms": "20",
        },
    ],
)
def test_alters(
    module_setup_tables,
    fn_setup_tables,
    with_append,
    depends_on,
    empty,
    database_name,
    settings,
):
    """
    Check correctness of functional states of RMV after CREATE, DROP, ALTER, trigger of RMV, ...
    """
    schedule_offset = opposite_minutes()
    create_sql = CREATE_RMV.render(
        table_name="test_rmv",
        if_not_exists=False,
        db="test_db",
        refresh_interval=f"EVERY 1 HOUR OFFSET {schedule_offset} MINUTE",
        depends_on=depends_on,
        to_clause="tgt1",
        select_query="SELECT * FROM src1",
        with_append=with_append,
        settings=settings,
    )
    node.query(create_sql)

    # Check same RMV is created on whole cluster
    def compare_DDL_on_all_nodes():
        show_create_all_nodes = cluster.query_all_nodes("SHOW CREATE test_rmv")
        assert_same_values(show_create_all_nodes.values())

    compare_DDL_on_all_nodes()

    node.query("DROP TABLE test_db.test_rmv")
    node.query(create_sql)
    compare_DDL_on_all_nodes()

    show_create = node.query("SHOW CREATE test_db.test_rmv")

    alter_sql = ALTER_RMV.render(
        table_name="test_rmv",
        if_not_exists=False,
        db="test_db",
        refresh_interval=f"EVERY 1 HOUR OFFSET {schedule_offset} MINUTE",
        depends_on=depends_on,
        # can't change select with alter
        # select_query="SELECT * FROM src1",
        with_append=with_append,
        settings=settings,
    )

    node.query(alter_sql)
    show_create_after_alter = node.query("SHOW CREATE test_db.test_rmv")
    assert show_create == show_create_after_alter
    compare_DDL_on_all_nodes()


def get_rmv_info(
    node,
    table,
    condition=None,
    max_attempts=50,
    delay=0.3,
    wait_status=None,
):
    def inner():
        rmv_info = node.query_with_retry(
            f"SELECT * FROM system.view_refreshes WHERE view='{table}'",
            check_callback=(
                (lambda r: r.iloc[0]["status"] == wait_status)
                if wait_status
                else (lambda r: r.iloc[0]["status"] != "Scheduling")
            ),
            parse=True,
        ).to_dict("records")[0]

        rmv_info["next_refresh_time"] = parse_ch_datetime(rmv_info["next_refresh_time"])
        rmv_info["last_success_time"] = parse_ch_datetime(rmv_info["last_success_time"])
        rmv_info["last_refresh_time"] = parse_ch_datetime(rmv_info["last_refresh_time"])
        logging.info(rmv_info)
        return rmv_info

    if condition:
        res = wait_condition(inner, condition, max_attempts=max_attempts, delay=delay)
        return res

    res = inner()
    return res


def parse_ch_datetime(date_str):
    if date_str is None:
        return None
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")


def expect_rows(rows, table="test_rmv"):
    inserted_data = node.query_with_retry(
        f"SELECT * FROM {table}",
        parse=True,
        check_callback=lambda x: len(x) == rows,
        retry_count=100,
    )
    assert len(inserted_data) == rows


def test_long_query_cancel(fn_setup_tables):
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    create_sql = CREATE_RMV.render(
        table_name="test_rmv",
        refresh_interval="EVERY 5 SECONDS",
        to_clause="tgt1",
        select_query="SELECT now() a, sleep(1) b from numbers(5) settings max_block_size=1",
        with_append=False,
        empty=True,
        settings={"refresh_retries": "0"},
    )
    node.query(create_sql)

    done = False
    start = time.time()
    while not done:
        for n in nodes:
            n.query("SYSTEM CANCEL VIEW test_rmv")
            if get_rmv_info(node2, "test_rmv")["exception"] == "cancelled":
                done = True

        time.sleep(0.1)
        if time.time() - start > 10:
            raise AssertionError("Can't cancel query")

    rmv = get_rmv_info(node, "test_rmv", wait_status="Scheduled")
    assert rmv["status"] == "Scheduled"
    assert rmv["exception"] == "cancelled"
    assert rmv["last_success_time"] is None

    assert node.query("SELECT count() FROM tgt1") == "0\n"

    get_rmv_info(node, "test_rmv", delay=0.1, max_attempts=1000, wait_status="Running")
    get_rmv_info(
        node, "test_rmv", delay=0.1, max_attempts=1000, wait_status="Scheduled"
    )

    assert node.query("SELECT count() FROM tgt1") == "5\n"


@pytest.fixture(scope="function")
def fn3_setup_tables():
    node.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS test_db.test_rmv")
    node.query("DROP TABLE IF EXISTS tgt1 ON CLUSTER default SYNC")

    node.query(
        "CREATE TABLE tgt1 ON CLUSTER default (a DateTime) ENGINE = ReplicatedMergeTree ORDER BY tuple()"
    )

    yield

    # A leaked test_rmv keeps retrying every 2 seconds, and each failed attempt creates and drops
    # a temp table, so later tests cannot enqueue their own DDL (Code 529). Stop the refresher on
    # both replicas before the DROP, which is itself replicated DDL. SYSTEM STOP VIEW is a local
    # no-op when the view does not exist, so it needs no guard.
    for n in nodes:
        n.query("SYSTEM STOP VIEW test_rmv")
    node.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER default SYNC")


def test_query_fail(fn3_setup_tables):
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    create_sql = CREATE_RMV.render(
        table_name="test_rmv",
        refresh_interval="EVERY 1 HOUR",
        to_clause="tgt1",
        # Argument at index 1 for function throwIf must be constant
        select_query="SELECT throwIf(1, toString(rand())) a",
        with_append=False,
        on_cluster="default",
        empty=True,
        settings={
            "refresh_retries": "10",
        },
    )
    with pytest.raises(helpers.client.QueryRuntimeException) as exc:
        node.query(create_sql)
        assert "Argument at index 1 for function throwIf must be constant" in str(
            exc.value
        )
    assert (
        node.query("SELECT count() FROM system.view_refreshes WHERE view='test_rmv'")
        == "0\n"
    )
    assert (
        node.query("SELECT count() FROM system.tables WHERE name='test_rmv'") == "0\n"
    )


def test_query_retry(fn3_setup_tables):
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    create_sql = CREATE_RMV.render(
        table_name="test_rmv",
        refresh_interval="EVERY 2 SECOND",
        to_clause="tgt1",
        select_query="SELECT throwIf(1, '111') a",
        with_append=False,
        on_cluster="default",
        empty=True,
        settings={
            "refresh_retries": "10",
            "refresh_retry_initial_backoff_ms": "1",
            "refresh_retry_max_backoff_ms": "1",
        },
    )
    node.query(create_sql)
    rmv = get_rmv_info(
        node,
        "test_rmv",
        delay=0.1,
        max_attempts=1000,
        condition=lambda x: x["retry"] == 11,
    )
    assert rmv["retry"] == 11
    assert "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO" in rmv["exception"]


def _drop_circular_objects():
    # Quiesce the refresh cycle before touching the schema. While the cycle runs, every
    # non-APPEND refresh of current_batch_v enqueues replicated-DDL entries into the
    # Replicated database's log (create + exchange + drop of the swap table), and under CI
    # load a replica can fall more than max_replication_lag_to_enqueue entries behind, after
    # which any DDL on it fails with "Cannot enqueue query on this replica, because it has
    # replication lag of N queries" (NOT_A_LEADER).
    #
    # The stop must survive a lagging replica catching up. SYSTEM STOP VIEW only installs a
    # local action lock on a replica where the view is already attached, so a replica that has
    # not replayed the CREATE yet would attach the view during the SYNC below and start
    # refreshing unpaused. SYSTEM STOP REPLICATED VIEW instead writes a persistent "paused"
    # znode into the view's Keeper coordination state, which every replica checks before
    # scheduling a refresh - including a replica that only attaches the view afterwards. All
    # three views are coordinated (Replicated database, no all_replicas setting), so issuing
    # it from any one replica that has the view attached pauses the whole cycle. The views may
    # not exist yet (the first call precedes creation) or may not be attached on a given
    # replica yet, hence the fallback to the other replica. Only that absence error is
    # tolerated - SYSTEM STOP REPLICATED VIEW on a replica whose RefreshSet has no such view
    # throws BAD_ARGUMENTS "Refreshable view ... doesn't exist" (there is no table lookup
    # before the RefreshSet lookup, so no UNKNOWN_TABLE). Any other failure to pause a live
    # view would leak the running cycle into the rest of the module, so it is re-raised
    # instead of letting cleanup continue and mask the root cause behind later DDL failures.
    for v in ("current_batch_v", "batch_log_v", "stats_v"):
        unexpected_error = None
        for n in nodes:
            try:
                n.query(f"SYSTEM STOP REPLICATED VIEW {v}")
                unexpected_error = None
                break
            except helpers.client.QueryRuntimeException as e:
                # 36 = BAD_ARGUMENTS, thrown by InterpreterSystemQuery::getRefreshTasks
                if e.returncode == 36 and "doesn't exist" in str(e):
                    continue
                unexpected_error = e
        if unexpected_error is not None:
            raise unexpected_error
    for n in nodes:
        n.query("SYSTEM SYNC DATABASE REPLICA default")

    node.query("DROP TABLE IF EXISTS current_batch_v ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS batch_log_v ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS stats_v ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS current_batch ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS batch_log ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS stats ON CLUSTER default SYNC")


def _wait_batch_log_max_t(at_least, timeout=120):
    """Wait until batch_log's frontier max(max_t) reaches `at_least`, polling either node.

    Progress is measured by the frontier rather than the row count because the dependency
    cycle can occasionally append a wave twice (see test_circular_dependencies_survive_restart);
    a duplicate append grows the row count without advancing the cycle.
    """
    deadline = time.time() + timeout
    last = 0
    while time.time() < deadline:
        for n in nodes:
            try:
                frontier = int(n.query("SELECT max(max_t) FROM batch_log").strip())
            except Exception:
                frontier = 0
            if frontier >= at_least:
                return frontier
            last = max(last, frontier)
        time.sleep(0.5)
    raise AssertionError(
        f"batch_log max_t did not reach {at_least} within {timeout}s; last seen {last}"
    )


def test_circular_dependencies_survive_restart(module_setup_tables):
    """3-view circular refresh chain (current_batch → batch_log, stats → current_batch).

    The cycle must:
      * keep going by itself once kicked (no further SYSTEM REFRESH VIEW required), and
      * survive a full cluster restart (in Replicated DB the dependency state is in Keeper, so
        the cycle resumes without a manual kick after restart).
    """
    _drop_circular_objects()

    node.query(
        "CREATE TABLE current_batch ON CLUSTER default (t UInt64, v Int64) "
        "ENGINE = ReplicatedMergeTree ORDER BY t"
    )
    node.query(
        "CREATE TABLE batch_log ON CLUSTER default (max_t UInt64, n Int64) "
        "ENGINE = ReplicatedMergeTree ORDER BY max_t"
    )
    node.query(
        "CREATE TABLE stats ON CLUSTER default (h UInt64, n UInt64) "
        "ENGINE = ReplicatedSummingMergeTree ORDER BY h"
    )

    # Reader: REFRESH AFTER 1 SECOND DEPENDS ON loggers.
    node.query(
        "CREATE MATERIALIZED VIEW current_batch_v "
        "REFRESH AFTER 1 SECOND DEPENDS ON batch_log_v, stats_v TO current_batch AS "
        "SELECT number AS t, number * 10 AS v FROM system.numbers "
        "WHERE number > (SELECT max(max_t) FROM batch_log) LIMIT 5"
    )
    node.query(
        "CREATE MATERIALIZED VIEW batch_log_v "
        "REFRESH DEPENDS ON current_batch_v APPEND TO batch_log AS "
        "SELECT max(t) AS max_t, count() AS n FROM current_batch"
    )
    node.query(
        "CREATE MATERIALIZED VIEW stats_v "
        "REFRESH DEPENDS ON current_batch_v APPEND TO stats AS "
        "SELECT cityHash64(v) % 8 AS h, count() AS n FROM current_batch GROUP BY h"
    )

    # From here on the cycle is live and keeps enqueueing replicated-DDL entries on every wave,
    # so the cleanup must run even when an assertion fails — a leaked cycle starves the DDL
    # queue and makes every later test in the module fail with "Cannot enqueue query on this
    # replica" (NOT_A_LEADER).
    try:
        # Kick the cycle once. Subsequent waves must run without further intervention. Each wave
        # advances the frontier max(max_t) by exactly 5 (every refresh reads 5 fresh numbers), so 3
        # self-sustained waves means the frontier reaches at least 15.
        node.query("SYSTEM REFRESH VIEW current_batch_v")

        pre_max = _wait_batch_log_max_t(15)
        assert pre_max >= 15

        # Full cluster restart. With Replicated DB, dependency state is persisted in Keeper, so the
        # cycle should resume on its own and push the frontier further without another manual kick.
        for n in nodes:
            n.restart_clickhouse()

        post_max = _wait_batch_log_max_t(pre_max + 15)
        assert post_max >= pre_max + 15

        # Sanity-check the wave invariants. The cycle can occasionally re-run a wave (e.g. an extra
        # refresh right after restart re-reads current_batch before it advances), and since the loggers
        # are APPEND views such a re-run produces a duplicate row. So max_t is not required to be
        # unique. What must hold: max_t never goes backwards, the distinct waves are exactly the gapless
        # progression 5, 10, 15, ... (no skipped or spurious wave), and every wave has a positive count.
        #
        # post_max may have been observed on either replica by _wait_batch_log_max_t, but the invariants
        # below are read from node1. batch_log is a ReplicatedMergeTree, so node1 may not have fetched
        # the latest part yet; sync it first so it has caught up to at least post_max. Otherwise the
        # distinct[-1] >= post_max check could spuriously fail under replication lag.
        node.query("SYSTEM SYNC REPLICA batch_log")
        rows = node.query(
            "SELECT max_t, n FROM batch_log ORDER BY max_t FORMAT TabSeparated"
        ).strip().split("\n")
        parsed = [tuple(int(x) for x in row.split("\t")) for row in rows]
        max_ts = [mt for mt, _ in parsed]
        assert max_ts == sorted(max_ts), f"max_t went backwards: {parsed}"
        distinct = sorted(set(max_ts))
        assert distinct == list(
            range(5, distinct[-1] + 1, 5)
        ), f"distinct waves are not the gapless 5, 10, 15, ... progression: {parsed}"
        assert distinct[-1] >= post_max, f"frontier regressed below {post_max}: {parsed}"
        assert all(n > 0 for _, n in parsed), f"some waves had n<=0: {parsed}"
    finally:
        _drop_circular_objects()


def _drop_sync_objects():
    node.query("DROP TABLE IF EXISTS child_v ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS parent_v ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS sync_src ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS parent_tbl ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS child_tbl ON CLUSTER default SYNC")


@pytest.mark.parametrize("with_append_parent", [True, False])
def test_dependent_sees_latest_data_other_replica(module_setup_tables, with_append_parent):
    """When the dependent runs on a different replica from the dependency's last refresh, it
    must still see the dependency's latest data via syncForDependentRefresh.

    Setup: parent refresh writes data, child depends on parent and reads it. We force parent
    to refresh on node1 only, then PAUSE the child on node1 so the child can only refresh on
    node2 — which is the replica that did NOT run parent. The child must see the parent data.
    Covered for both APPEND parent (sync via SYNC REPLICA) and non-APPEND parent (sync via
    waiting for the new inner-table UUID).
    """
    _drop_sync_objects()

    node.query(
        "CREATE TABLE sync_src ON CLUSTER default (v Int64) "
        "ENGINE = ReplicatedMergeTree ORDER BY v"
    )
    node.query(
        "CREATE TABLE child_tbl ON CLUSTER default (max_v Int64) "
        "ENGINE = ReplicatedMergeTree ORDER BY max_v"
    )

    if with_append_parent:
        # Parent is APPEND TO an explicit table; child reads that table.
        node.query(
            "CREATE TABLE parent_tbl ON CLUSTER default (v Int64) "
            "ENGINE = ReplicatedMergeTree ORDER BY v"
        )
        node.query(
            "CREATE MATERIALIZED VIEW parent_v "
            "REFRESH AFTER 1 YEAR APPEND TO parent_tbl EMPTY AS "
            "SELECT v FROM sync_src"
        )
        # syncForDependentRefresh in APPEND mode: SYNC REPLICA on parent_tbl before child refresh.
        child_select = "SELECT max(v) AS max_v FROM parent_tbl"
    else:
        # Parent is non-APPEND with its own atomically-swapped Replicated inner table.
        node.query(
            "CREATE MATERIALIZED VIEW parent_v "
            "REFRESH AFTER 1 YEAR ENGINE = ReplicatedMergeTree ORDER BY v EMPTY AS "
            "SELECT v FROM sync_src"
        )
        # syncForDependentRefresh in non-APPEND mode: wait for the new inner-table UUID to appear.
        child_select = "SELECT max(v) AS max_v FROM parent_v"

    node.query(
        "CREATE MATERIALIZED VIEW child_v "
        "REFRESH DEPENDS ON parent_v APPEND TO child_tbl EMPTY AS " + child_select
    )

    # Pin parent to node1 only, child to node2 only. Child on node1 must be PAUSED (not STOPPED)
    # so that subsequent SYSTEM START VIEW would resume it; STOP would also work for this test
    # but PAUSE exercises the same code path the test description targets.
    node.query("SYSTEM PAUSE VIEW child_v")

    # First wave: parent runs on node1, child runs on node2.
    node.query("INSERT INTO sync_src VALUES (1)")
    node.query("SYSTEM REFRESH VIEW parent_v")
    # Wait for parent on node1 to complete a refresh.
    get_rmv_info(
        node,
        "parent_v",
        delay=0.1,
        max_attempts=600,
        condition=lambda x: x["last_success_time"] is not None,
    )
    parent_replica1 = node.query(
        "SELECT last_refresh_replica FROM system.view_refreshes WHERE view='parent_v'"
    ).strip()
    assert parent_replica1 != "", "parent's last_refresh_replica should be populated"

    # Wait for child to refresh on node2 (the only replica where child is enabled).
    get_rmv_info(
        node2,
        "child_v",
        delay=0.1,
        max_attempts=600,
        condition=lambda x: x["last_success_time"] is not None,
    )
    child_replica1 = node2.query(
        "SELECT last_refresh_replica FROM system.view_refreshes WHERE view='child_v'"
    ).strip()
    assert (
        parent_replica1 != child_replica1
    ), f"parent and child should have run on different replicas, got {parent_replica1} for both"

    # Verify the child sees v=1 — propagated from node1 through syncForDependentRefresh on node2.
    seen_max = 0
    for _ in range(60):
        seen_max = int(node.query("SELECT max(max_v) FROM child_tbl").strip() or "0")
        if seen_max == 1:
            break
        time.sleep(0.5)
    assert seen_max == 1, f"child_tbl missed parent's v=1, max_v={seen_max}"

    # Second wave: insert v=2, wait for parent's next refresh on node1 and child's next on node2.
    parent_last_success_before = node.query(
        "SELECT last_success_time FROM system.view_refreshes WHERE view='parent_v'"
    ).strip()
    child_last_success_before = node2.query(
        "SELECT last_success_time FROM system.view_refreshes WHERE view='child_v'"
    ).strip()
    node.query("INSERT INTO sync_src VALUES (2)")
    time.sleep(1.5) # make sure the two refreshes get different %H:%M:%S timestamps
    node.query("SYSTEM REFRESH VIEW parent_v")

    get_rmv_info(
        node,
        "parent_v",
        delay=0.1,
        max_attempts=600,
        condition=lambda x: (
            x["last_success_time"] is not None
            and x["last_success_time"].strftime("%Y-%m-%d %H:%M:%S")
            != parent_last_success_before
        ),
    )
    get_rmv_info(
        node2,
        "child_v",
        delay=0.1,
        max_attempts=600,
        condition=lambda x: (
            x["last_success_time"] is not None
            and x["last_success_time"].strftime("%Y-%m-%d %H:%M:%S")
            != child_last_success_before
        ),
    )

    # Final check: child saw v=2 even though parent's refresh happened on node1 only.
    final_max = 0
    for _ in range(60):
        final_max = int(node.query("SELECT max(max_v) FROM child_tbl").strip() or "0")
        if final_max == 2:
            break
        time.sleep(0.5)
    assert final_max == 2, f"child_tbl missed parent's v=2, max_v={final_max}"

    # Cleanup. Restore both views first so DROP doesn't get tangled with PAUSE/STOP state.
    node.query("SYSTEM START VIEW child_v")
    node2.query("SYSTEM START VIEW parent_v")
    _drop_sync_objects()


def test_wait_view_covers_refresh_requested_on_another_replica(fn3_setup_tables):
    # `SYSTEM WAIT VIEW` must cover a `SYSTEM REFRESH VIEW` another replica accepted but hasn't started.
    # The request is queued behind a running refresh, so it exists without an attempt for that long.
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    create_sql = CREATE_RMV.render(
        table_name="test_rmv",
        refresh_interval="EVERY 1 HOUR",
        to_clause="tgt1",
        # 5 rows, one second each: every refresh takes ~5s and appends 5 rows.
        select_query="SELECT now() + sleepEachRow(1) a FROM numbers(5) SETTINGS max_block_size = 1",
        with_append=True,
        on_cluster="default",
        empty=True,
        settings={"refresh_retries": "0"},
    )
    node.query(create_sql)

    node.query("SYSTEM REFRESH VIEW test_rmv")
    get_rmv_info(node, "test_rmv", wait_status="Running")
    # node2 has read the current coordination state, so this is not about a stale cache.
    get_rmv_info(node2, "test_rmv", wait_status="RunningOnAnotherReplica")

    # Accepted by `node` while refresh #1 is still running, so it cannot start yet.
    node.query("SYSTEM REFRESH VIEW test_rmv")

    node2.query("SYSTEM WAIT VIEW test_rmv", timeout=180)

    # Both refreshes must be done by now: 5 rows each.
    rows = node.query("SELECT count() FROM tgt1").strip()
    assert rows == "10", f"node2 stopped waiting after {rows} rows, expected 10"


def test_wait_view_covers_request_before_any_attempt_starts(fn3_setup_tables):
    # The same before any attempt exists: the refresh is requested on `node` and its coordination
    # write is parked, so nothing is running anywhere. A wait on node2 must still cover the request.
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    create_sql = CREATE_RMV.render(
        table_name="test_rmv",
        refresh_interval="EVERY 1 HOUR",
        to_clause="tgt1",
        select_query="SELECT now() a FROM numbers(5)",
        with_append=True,
        on_cluster="default",
        empty=True,
        settings={"refresh_retries": "0"},
    )
    node.query(create_sql)

    fp = "refresh_mv_pause_inside_coordination_write"
    node.query(f"SYSTEM ENABLE FAILPOINT {fp}")
    released = False
    try:
        node.query("SYSTEM REFRESH VIEW test_rmv")
        # `node` is now parked before publishing the attempt to Keeper.
        node.query(f"SYSTEM WAIT FAILPOINT {fp} PAUSE")
        assert node.query("SELECT count() FROM tgt1").strip() == "0"

        wait_done = []

        def wait_on_node2():
            node2.query("SYSTEM WAIT VIEW test_rmv", timeout=180)
            wait_done.append(time.time())

        waiter = threading.Thread(target=wait_on_node2)
        waiter.start()
        try:
            # The wait must not finish while the requested refresh has not run.
            time.sleep(3)
            assert not wait_done, "node2 stopped waiting before the requested refresh ran"
        finally:
            node.query(f"SYSTEM DISABLE FAILPOINT {fp}")
            released = True
            waiter.join(timeout=180)

        assert wait_done, "node2 never finished waiting"
        rows = node.query("SELECT count() FROM tgt1").strip()
        assert rows == "5", f"node2 stopped waiting after {rows} rows, expected 5"
    finally:
        if not released:
            node.query(f"SYSTEM DISABLE FAILPOINT {fp}")


def test_drop_view_with_unconsumed_refresh_request(fn3_setup_tables):
    # A request made while the view is stopped cluster-wide is never consumed. Dropping the view must
    # still remove the coordination znode, which would fail with a request znode left under it.
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    create_sql = CREATE_RMV.render(
        table_name="test_rmv",
        refresh_interval="EVERY 1 HOUR",
        to_clause="tgt1",
        select_query="SELECT now() a",
        with_append=True,
        on_cluster="default",
        empty=True,
    )
    node.query(create_sql)

    uuid = node.query(
        "SELECT uuid FROM system.tables WHERE database = 'default' AND name = 'test_rmv'"
    ).strip()
    znode_path = f"/clickhouse/tables/{uuid}/1"

    node.query("SYSTEM STOP REPLICATED VIEW test_rmv")
    for n in nodes:
        get_rmv_info(n, "test_rmv", wait_status="Disabled")

    # Accepted and published, but the cluster-wide stop means it can never run.
    node.query("SYSTEM REFRESH VIEW test_rmv")
    wait_condition(
        lambda: node.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{znode_path}'"
            " AND name LIKE 'request-%'"
        ).strip(),
        lambda x: x == "1",
        max_attempts=50,
        delay=0.2,
    )

    node.query("DROP TABLE test_rmv ON CLUSTER default SYNC")

    # `system.zookeeper` throws instead of returning nothing for a path that is gone, and the
    # parent may be gone too, so ask the parent for the child and treat either absence as removed.
    def coordination_znode_gone():
        try:
            return (
                node.query(
                    f"SELECT count() FROM system.zookeeper"
                    f" WHERE path = '/clickhouse/tables/{uuid}' AND name = '1'"
                ).strip()
                == "0"
            )
        except helpers.client.QueryRuntimeException:
            return True

    assert coordination_znode_gone(), "the view's coordination znode was left behind in Keeper"


def test_wait_view_covers_request_on_another_replica_while_stopped_locally(
    fn3_setup_tables,
):
    # A replica stopped with `SYSTEM STOP VIEW` or `SYSTEM PAUSE VIEW` is Disabled: no refresh starts
    # *there*. Another replica's request is run by that replica, so `SYSTEM WAIT VIEW` here must wait.
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    create_sql = CREATE_RMV.render(
        table_name="test_rmv",
        refresh_interval="EVERY 1 HOUR",
        to_clause="tgt1",
        select_query="SELECT now() a FROM numbers(5)",
        with_append=True,
        on_cluster="default",
        empty=True,
        settings={"refresh_retries": "0"},
    )
    node.query(create_sql)

    uuid = node.query(
        "SELECT uuid FROM system.tables WHERE database = 'default' AND name = 'test_rmv'"
    ).strip()
    znode_path = f"/clickhouse/tables/{uuid}/1"

    # `node` holds on to the request until it is resumed, so it stays owed but not started; node2 -
    # the waiter - is stopped separately, so it is Disabled for a purely local reason.
    node.query("SYSTEM STOP VIEW test_rmv")
    node2.query("SYSTEM PAUSE VIEW test_rmv")
    for n in nodes:
        get_rmv_info(n, "test_rmv", wait_status="Disabled")

    node.query("SYSTEM REFRESH VIEW test_rmv")
    wait_condition(
        lambda: node2.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{znode_path}'"
            " AND name LIKE 'request-%'"
        ).strip(),
        lambda x: x == "1",
        max_attempts=50,
        delay=0.2,
    )

    wait_done = []

    def wait_on_node2():
        node2.query("SYSTEM WAIT VIEW test_rmv", timeout=180)
        wait_done.append(time.time())

    waiter = threading.Thread(target=wait_on_node2)
    waiter.start()
    try:
        time.sleep(3)
        assert not wait_done, "node2 stopped waiting before the requested refresh ran"
        assert node.query("SELECT count() FROM tgt1").strip() == "0"
    finally:
        node.query("SYSTEM START VIEW test_rmv")
        waiter.join(timeout=180)

    assert wait_done, "node2 never finished waiting"
    rows = node.query("SELECT count() FROM tgt1").strip()
    assert rows == "5", f"node2 stopped waiting after {rows} rows, expected 5"


def test_wait_view_returns_while_view_stopped_cluster_wide(fn3_setup_tables):
    # The other side: stopped cluster-wide, the request is deferred on every replica, so there is
    # nothing to wait for until `SYSTEM START REPLICATED VIEW`. `SYSTEM WAIT VIEW` must return.
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    create_sql = CREATE_RMV.render(
        table_name="test_rmv",
        refresh_interval="EVERY 1 HOUR",
        to_clause="tgt1",
        select_query="SELECT now() a FROM numbers(5)",
        with_append=True,
        on_cluster="default",
        empty=True,
        settings={"refresh_retries": "0"},
    )
    node.query(create_sql)

    uuid = node.query(
        "SELECT uuid FROM system.tables WHERE database = 'default' AND name = 'test_rmv'"
    ).strip()
    znode_path = f"/clickhouse/tables/{uuid}/1"

    node.query("SYSTEM STOP REPLICATED VIEW test_rmv")
    for n in nodes:
        get_rmv_info(n, "test_rmv", wait_status="Disabled")

    node.query("SYSTEM REFRESH VIEW test_rmv")
    wait_condition(
        lambda: node2.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{znode_path}'"
            " AND name LIKE 'request-%'"
        ).strip(),
        lambda x: x == "1",
        max_attempts=50,
        delay=0.2,
    )

    for n in nodes:
        n.query("SYSTEM WAIT VIEW test_rmv", timeout=30)

    assert node.query("SELECT count() FROM tgt1").strip() == "0"


def test_detach_view_retracts_unconsumed_refresh_request(started_cluster):
    # DETACH keeps the Keeper session alive, so a request znode would outlive the table and block other
    # replicas' `SYSTEM WAIT VIEW`. Publishing is parked so that the shutdown's last pass can't retract it.
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    try:
        # A Replicated database refuses `DETACH TABLE` and replicates `DETACH TABLE PERMANENTLY`, so
        # `DETACH DATABASE`, which is not replicated, is what detaches on one replica only.
        node.query(
            "CREATE DATABASE detach_db ON CLUSTER default"
            " ENGINE = Replicated('/clickhouse/detach_db/', '{shard}', '{replica}')"
        )
        node.query(
            "CREATE MATERIALIZED VIEW detach_db.mv REFRESH EVERY 1 HOUR"
            " ENGINE = ReplicatedMergeTree ORDER BY x EMPTY AS SELECT number AS x FROM numbers(5)"
        )
        uuid = node.query(
            "SELECT uuid FROM system.tables WHERE database = 'detach_db' AND name = 'mv'"
        ).strip()
        request_znodes = (
            f"SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/{uuid}/1'"
            " AND name LIKE 'request-%'"
        )

        fp = "refresh_mv_pause_before_publishing_refresh_request"
        node.query(f"SYSTEM ENABLE FAILPOINT {fp}")
        released = False
        detached = False
        try:
            requester = threading.Thread(
                target=lambda: node.query(
                    "SYSTEM REFRESH VIEW detach_db.mv", timeout=180
                )
            )
            requester.start()
            try:
                node.query(f"SYSTEM WAIT FAILPOINT {fp} PAUSE")
                node.query("DETACH DATABASE detach_db")
                detached = True
            finally:
                node.query(f"SYSTEM DISABLE FAILPOINT {fp}")
                released = True
                requester.join(timeout=180)
            assert (
                not requester.is_alive()
            ), "SYSTEM REFRESH VIEW got stuck behind the DETACH"

            # The request znode must not outlive the detached view.
            assert_eq_with_retry(node2, request_znodes, "0", retry_count=150, sleep_time=0.2)
            # Nothing is owed anymore, so node2 must not wait for the detached replica's request.
            node2.query("SYSTEM WAIT VIEW detach_db.mv", timeout=30)
        finally:
            if not released:
                node.query(f"SYSTEM DISABLE FAILPOINT {fp}")
            if detached:
                node.query("ATTACH DATABASE detach_db")
    finally:
        for n in nodes:
            n.query("DROP DATABASE IF EXISTS detach_db SYNC")
