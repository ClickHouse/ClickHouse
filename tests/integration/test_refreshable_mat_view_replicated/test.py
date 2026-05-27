import datetime
import logging
import time
import math
from datetime import datetime
from typing import Optional

import pytest
from jinja2 import Environment, Template

import helpers.client
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

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


@pytest.fixture(scope="session", autouse=True)
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
    node.query(f"DROP DATABASE IF EXISTS default ON CLUSTER default SYNC")
    node.query(
        "CREATE DATABASE default ON CLUSTER default ENGINE=Replicated('/clickhouse/default/','{shard}','{replica}')"
    )

    assert (
        node.query(
            f"SELECT engine FROM clusterAllReplicas(default, system.databases) where name='default'"
        )
        == "Replicated\nReplicated\n"
    )

    node.query(f"DROP DATABASE IF EXISTS test_db ON CLUSTER default SYNC")
    node.query(
        "CREATE DATABASE test_db ON CLUSTER default ENGINE=Replicated('/clickhouse/test_db/','{shard}','{replica}')"
    )

    assert (
        node.query(
            f"SELECT engine FROM clusterAllReplicas(default, system.databases) where name='test_db'"
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
        f"CREATE TABLE src1 ON CLUSTER default (a DateTime, b UInt64) ENGINE = ReplicatedMergeTree() ORDER BY tuple()"
    )
    node.query(
        f"CREATE TABLE src2 ON CLUSTER default (a DateTime, b UInt64) ENGINE = ReplicatedMergeTree() ORDER BY tuple()"
    )
    node.query(
        f"CREATE TABLE tgt1 ON CLUSTER default (a DateTime, b UInt64) ENGINE = ReplicatedMergeTree() ORDER BY tuple()"
    )
    node.query(
        f"CREATE TABLE tgt2 ON CLUSTER default (a DateTime, b UInt64) ENGINE = ReplicatedMergeTree() ORDER BY tuple()"
    )
    node.query(
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS dummy_rmv ON CLUSTER default "
        f"REFRESH EVERY 10 HOUR ENGINE = ReplicatedMergeTree() ORDER BY tuple() EMPTY AS select number as x from numbers(1)"
    )


@pytest.fixture(scope="function")
def fn_setup_tables():
    node.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS test_db.test_rmv")
    node.query("DROP TABLE IF EXISTS src1 ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS tgt1 ON CLUSTER default")

    node.query(
        f"CREATE TABLE tgt1 ON CLUSTER default (a DateTime, b UInt64) "
        f"ENGINE = ReplicatedMergeTree ORDER BY tuple()"
    )

    node.query(
        f"CREATE TABLE src1 ON CLUSTER default (a DateTime, b UInt64) "
        f"ENGINE = ReplicatedMergeTree ORDER BY tuple()"
    )
    node.query(f"INSERT INTO src1 VALUES ('2020-01-01', 1), ('2020-01-02', 2)")

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

    node.query(f"DROP TABLE test_db.test_rmv")
    node.query(create_sql)
    compare_DDL_on_all_nodes()

    show_create = node.query(f"SHOW CREATE test_db.test_rmv")

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
    show_create_after_alter = node.query(f"SHOW CREATE test_db.test_rmv")
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
        f"CREATE TABLE tgt1 ON CLUSTER default (a DateTime) ENGINE = ReplicatedMergeTree ORDER BY tuple()"
    )


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
        node.query(f"SELECT count() FROM system.view_refreshes WHERE view='test_rmv'")
        == "0\n"
    )
    assert (
        node.query(f"SELECT count() FROM system.tables WHERE name='test_rmv'") == "0\n"
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
    node.query("DROP TABLE IF EXISTS current_batch_v ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS batch_log_v ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS stats_v ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS current_batch ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS batch_log ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS stats ON CLUSTER default SYNC")


def _wait_batch_log_count(at_least, timeout=120):
    """Wait until batch_log has at least `at_least` rows, polling either node."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for n in nodes:
            try:
                count = int(n.query("SELECT count() FROM batch_log").strip())
            except Exception:
                count = 0
            if count >= at_least:
                return count
        time.sleep(0.5)
    raise AssertionError(
        f"batch_log did not reach {at_least} rows within {timeout}s; last seen {count}"
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

    # Kick the cycle once. Subsequent waves must run without further intervention.
    node.query("SYSTEM REFRESH VIEW current_batch_v")

    pre_count = _wait_batch_log_count(3)
    assert pre_count >= 3

    # Full cluster restart. With Replicated DB, dependency state is persisted in Keeper, so the
    # cycle should resume on its own and produce more waves without another manual kick.
    for n in nodes:
        n.restart_clickhouse()

    post_count = _wait_batch_log_count(pre_count + 3)
    assert post_count >= pre_count + 3

    # Sanity-check the wave invariants: max_t strictly increases and per-wave counts are positive.
    rows = node.query(
        "SELECT max_t, n FROM batch_log ORDER BY max_t FORMAT TabSeparated"
    ).strip().split("\n")
    parsed = [tuple(int(x) for x in row.split("\t")) for row in rows]
    assert len(parsed) == len({mt for mt, _ in parsed}), f"max_t not unique: {parsed}"
    assert parsed == sorted(parsed), f"max_t not monotonically increasing: {parsed}"
    assert all(n > 0 for _, n in parsed), f"some waves had n<=0: {parsed}"

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
