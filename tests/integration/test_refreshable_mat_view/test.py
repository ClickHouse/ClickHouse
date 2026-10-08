import datetime
import logging
import time
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
    macros={"shard": 1, "replica": 1},
)
node2 = cluster.add_instance(
    "node1_2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
)


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
    node.query(f"DROP DATABASE IF EXISTS test_db")
    node.query(f"CREATE DATABASE test_db ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS test_db.test_rmv ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS src1 ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS src2 ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS tgt1 ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS tgt2 ON CLUSTER default")

    node.query(
        f"CREATE TABLE src1 ON CLUSTER default (a DateTime, b UInt64) ENGINE = Memory"
    )
    node.query(
        f"CREATE TABLE src2 ON CLUSTER default (a DateTime, b UInt64) ENGINE = Memory"
    )
    node.query(
        f"CREATE TABLE tgt1 ON CLUSTER default (a DateTime, b UInt64) ENGINE = MergeTree ORDER BY tuple()"
    )
    node.query(
        f"CREATE TABLE tgt2 ON CLUSTER default (a DateTime, b UInt64) ENGINE = Memory"
    )
    node.query(
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS dummy_rmv ON CLUSTER default "
        f"REFRESH EVERY 10 HOUR engine Memory EMPTY AS select number as x from numbers(1)"
    )


@pytest.fixture(scope="function")
def fn_setup_tables():
    node.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS test_db.test_rmv ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS src1 ON CLUSTER default")
    node.query("DROP TABLE IF EXISTS tgt1 ON CLUSTER default")

    node.query(
        f"CREATE TABLE tgt1 ON CLUSTER default (a DateTime, b UInt64) ENGINE = MergeTree ORDER BY tuple()"
    )

    node.query(
        f"CREATE TABLE src1 ON CLUSTER default (a DateTime, b UInt64) ENGINE = Memory"
    )
    node.query(f"INSERT INTO src1 VALUES ('2020-01-01', 1), ('2020-01-02', 2)")


def opposite_minutes():
    return (60 - datetime.now().minute) % 60


@pytest.mark.parametrize(
    "select_query",
    [
        "SELECT now() as a, number as b FROM numbers(2)",
        "SELECT now() as a, b as b FROM src1",
    ],
)
@pytest.mark.parametrize("with_append", [True, False])
@pytest.mark.parametrize("empty", [True, False])
def test_simple_append(
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

    if empty:
        expect = "2\n"

    if not with_append:
        expect = "2\n"

    if with_append and not empty:
        expect = "4\n"

    records = node.query_with_retry(
        "SELECT count() FROM test_rmv", check_callback=lambda x: x == expect
    )
    assert records == expect


@pytest.mark.parametrize("with_append", [True, False])
@pytest.mark.parametrize("on_cluster", [True, False])
@pytest.mark.parametrize("depends_on", [None, ["default.dummy_rmv"]])
@pytest.mark.parametrize("empty", [True, False])
@pytest.mark.parametrize("database_name", [None, "test_db"])
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
    on_cluster,
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
        db=database_name,
        refresh_interval=f"EVERY 1 HOUR OFFSET {schedule_offset} MINUTE",
        depends_on=depends_on,
        to_clause="tgt1",
        select_query="SELECT * FROM src1",
        with_append=with_append,
        on_cluster="default" if on_cluster else None,
        settings=settings,
    )
    node.query(create_sql)

    # Check same RMV is created on whole cluster
    def compare_DDL_on_all_nodes():
        show_create_all_nodes = cluster.query_all_nodes("SHOW CREATE test_rmv")

        if not on_cluster:
            del show_create_all_nodes["node1_2"]

        assert_same_values(show_create_all_nodes.values())

    compare_DDL_on_all_nodes()

    maybe_db = f"{database_name}." if database_name else ""
    node.query(
        f"DROP TABLE {maybe_db}test_rmv {'ON CLUSTER default' if on_cluster else ''}"
    )
    node.query(create_sql)
    compare_DDL_on_all_nodes()

    show_create = node.query(f"SHOW CREATE {maybe_db}test_rmv")

    alter_sql = ALTER_RMV.render(
        table_name="test_rmv",
        if_not_exists=False,
        db=database_name,
        refresh_interval=f"EVERY 1 HOUR OFFSET {schedule_offset} MINUTE",
        depends_on=depends_on,
        # can't change select with alter
        # select_query="SELECT * FROM src1",
        with_append=with_append,
        on_cluster="default" if on_cluster else None,
        settings=settings,
    )

    node.query(alter_sql)
    show_create_after_alter = node.query(f"SHOW CREATE {maybe_db}test_rmv")
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
            retry_count=max_attempts,
            sleep_time=delay,
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


def test_long_query(fn_setup_tables):
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    create_sql = CREATE_RMV.render(
        table_name="test_rmv",
        refresh_interval=f"EVERY 1 HOUR OFFSET {opposite_minutes()} MINUTE",
        to_clause="tgt1",
        select_query="SELECT now() a, sleep(1) b from numbers(10) settings max_block_size=1",
        with_append=False,
        empty=True,
    )
    node.query(create_sql)
    rmv = get_rmv_info(node, "test_rmv")
    node.query(f"SYSTEM TEST VIEW test_rmv SET FAKE TIME '{rmv['next_refresh_time']}'")

    wait_seconds = 0
    progresses = []
    while wait_seconds < 30:
        rmv = get_rmv_info(node, "test_rmv")
        if rmv["progress"] == 1:
            break

        time.sleep(0.01)
        wait_seconds += 0.01

        # didn't start yet
        if rmv["status"] == "Scheduled" or rmv["progress"] == 0:
            continue

        assert rmv["status"] == "Running"
        assert 0 < rmv["read_rows"] < 100000000
        assert 0 < rmv["read_bytes"] < 1200000000
        progresses.append(rmv["progress"])

    assert rmv["progress"] == 1
    assert rmv["exception"] is None

    assert any(0 < p < 1 for p in progresses)


def test_long_query_cancel(fn_setup_tables):
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    create_sql = CREATE_RMV.render(
        table_name="test_rmv",
        refresh_interval="EVERY 3 SECONDS",
        to_clause="tgt1",
        select_query="SELECT now() a, sleep(1) b from numbers(5) settings max_block_size=1",
        with_append=False,
        empty=True,
        settings={"refresh_retries": "0"},
    )
    node.query(create_sql)
    get_rmv_info(node, "test_rmv", delay=0.1, max_attempts=1000, wait_status="Running")

    node.query("SYSTEM CANCEL VIEW test_rmv")
    rmv = get_rmv_info(
        node, "test_rmv", delay=0.1, max_attempts=1000, wait_status="Scheduled"
    )
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
    node.query("DROP TABLE IF EXISTS test_db.test_rmv ON CLUSTER default SYNC")
    node.query("DROP TABLE IF EXISTS tgt1 ON CLUSTER default")

    node.query(f"CREATE TABLE tgt1 ON CLUSTER default (a DateTime) ENGINE = Memory")


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
