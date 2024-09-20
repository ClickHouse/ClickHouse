import datetime
import logging
import re
import time
from typing import Optional

import pytest

from jinja2 import Template, Environment
from datetime import datetime, timedelta
import helpers.client
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, assert_logs_contain, wait_condition
from test_refreshable_mat_view.schedule_model import (
    get_next_refresh_time,
    compare_dates,
    compare_dates_,
)


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

uuid_regex = re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def assert_create_query(nodes, table_name, expected):
    replace_uuid = lambda x: re.sub(uuid_regex, "uuid", x)
    query = "SHOW CREATE TABLE {}".format(table_name)
    for node in nodes:
        assert_eq_with_retry(node, query, expected, get_result=replace_uuid)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(scope="module", autouse=True)
def setup_tables(started_cluster):
    print(node.query("SELECT version()"))


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
22. ON CLUSTER (do when database replicated support is ready)

+SYSTEM STOP|START|REFRESH|CANCEL VIEW
SYSTEM WAIT VIEW [db.]name

view_refreshes:
+progress
+elapsed
refresh_count
+exception

+progress: inf if reading from table
+1 if reading from numbers()

RMV settings
+ * `refresh_retries` - How many times to retry if refresh query fails with an exception. If all retries fail, skip to the next scheduled refresh time. 0 means no retries, -1 means infinite retries. Default: 0.
 * `refresh_retry_initial_backoff_ms` - Delay before the first retry, if `refresh_retries` is not zero. Each subsequent retry doubles the delay, up to `refresh_retry_max_backoff_ms`. Default: 100 ms.
 * `refresh_retry_max_backoff_ms` - Limit on the exponential growth of delay between refresh attempts. Default: 60000 ms (1 minute).

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

CREATE_RMV_TEMPLATE = j2_template(
    """CREATE MATERIALIZED VIEW
{% if if_not_exists %}IF NOT EXISTS{% endif %}
{% if db %}{{db}}.{% endif %}{{ table_name }}
{% if on_cluster %}ON CLUSTER {{ on_cluster }}{% endif %}
REFRESH
"""
    + RMV_TEMPLATE
)

# ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
ALTER_RMV_TEMPLATE = j2_template(
    """ALTER TABLE
{% if db %}{{db}}.{% endif %}{{ table_name }}
{% if on_cluster %}ON CLUSTER {{ on_cluster }}{% endif %}
MODIFY REFRESH
"""
    + RMV_TEMPLATE
)


@pytest.fixture(scope="module")
def module_setup_tables():
    node.query(f"CREATE DATABASE IF NOT EXISTS test_db ON CLUSTER test_cluster ENGINE = Atomic")

    node.query(
        f"CREATE TABLE src1 ON CLUSTER test_cluster (a DateTime, b UInt64) ENGINE = Memory"
    )
    node.query(
        f"CREATE TABLE src2 ON CLUSTER test_cluster (a DateTime, b UInt64) ENGINE = Memory"
    )
    node.query(
        f"CREATE TABLE tgt1 ON CLUSTER test_cluster (a DateTime, b UInt64) ENGINE = MergeTree ORDER BY tuple()"
    )
    node.query(
        f"CREATE TABLE tgt2 ON CLUSTER test_cluster (a DateTime, b UInt64) ENGINE = Memory"
    )
    node.query(
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS dummy_rmv ON CLUSTER test_cluster "
        f"REFRESH EVERY 10 HOUR engine Memory EMPTY AS select number as x from numbers(1)"
    )
    yield
    node.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER test_cluster")
    node.query("DROP TABLE IF EXISTS src1 ON CLUSTER test_cluster")
    node.query("DROP TABLE IF EXISTS src2 ON CLUSTER test_cluster")
    node.query("DROP TABLE IF EXISTS tgt1 ON CLUSTER test_cluster")
    node.query("DROP TABLE IF EXISTS tgt2 ON CLUSTER test_cluster")
    node.query("DROP DATABASE IF EXISTS test_db ON CLUSTER test_cluster")


@pytest.fixture(scope="function")
def fn_setup_tables():
    node.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER test_cluster")
    node.query("DROP TABLE IF EXISTS test_db.test_rmv ON CLUSTER test_cluster")
    yield


def test_simple_append(module_setup_tables, fn_setup_tables):
    create_sql = CREATE_RMV_TEMPLATE.render(
        table_name="test_rmv",
        refresh_interval="EVERY 1 HOUR",
        to_clause="tgt1",
        select_query="SELECT now() as a, number as b FROM numbers(2)",
        with_append=True,
        empty=False,
    )
    node.query(create_sql)
    rmv = get_rmv_info(node, "test_rmv")
    assert rmv["exception"] is None

    node.query(f"SYSTEM TEST VIEW test_rmv SET FAKE TIME '{rmv['next_refresh_time']}'")
    time.sleep(2)
    rmv2 = get_rmv_info(
        node,
        "test_rmv",
    )

    assert rmv2["exception"] is None


def test_simple_append_from_table(fn2_setup_tables):
    create_sql = CREATE_RMV_TEMPLATE.render(
        table_name="test_rmv",
        refresh_interval="EVERY 1 HOUR",
        to_clause="tgt1",
        select_query="SELECT now() as a, b as b FROM src1",
        with_append=True,
    )
    node.query(create_sql)
    rmv = get_rmv_info(node, "test_rmv")

    node.query(f"SYSTEM TEST VIEW test_rmv SET FAKE TIME '{rmv['next_refresh_time']}'")
    time.sleep(2)
    rmv2 = get_rmv_info(
        node,
        "test_rmv",
    )

    assert rmv2["exception"] is None


@pytest.mark.parametrize("with_append", [True, False])
@pytest.mark.parametrize("if_not_exists", [True, False])
@pytest.mark.parametrize("on_cluster", [True, False])
@pytest.mark.parametrize("depends_on", [None, ["default.dummy_rmv"]])
@pytest.mark.parametrize("empty", [True, False])
@pytest.mark.parametrize("database_name", [None, "default", "test_db"])
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
    started_cluster,
    with_append,
    if_not_exists,
    on_cluster,
    depends_on,
    empty,
    database_name,
    settings,
):
    """
    Check correctness of functional states of RMV after CREATE, DROP, ALTER, trigger of RMV, ...
    """
    create_sql = CREATE_RMV_TEMPLATE.render(
        table_name="test_rmv",
        if_not_exists=if_not_exists,
        db=database_name,
        refresh_interval="EVERY 1 HOUR",
        depends_on=depends_on,
        to_clause="tgt1",
        select_query="SELECT * FROM src1",
        with_append=with_append,
        on_cluster="test_cluster" if on_cluster else None,
        settings=settings,
    )
    node.query(create_sql)

    # Check same RMV is created on cluster
    def compare_DDL_on_all_nodes():
        show_create_all_nodes = cluster.query_all_nodes("SHOW CREATE test_rmv")

        if not on_cluster:
            del show_create_all_nodes["node1_2"]

        assert_same_values(show_create_all_nodes.values())

    compare_DDL_on_all_nodes()

    maybe_db = f"{database_name}." if database_name else ""
    node.query(f"DROP TABLE {maybe_db}test_rmv {'ON CLUSTER test_cluster' if on_cluster else ''}")
    node.query(create_sql)
    compare_DDL_on_all_nodes()

    show_create = node.query(f"SHOW CREATE {maybe_db}test_rmv")

    # Alter of RMV replaces all non-specified
    alter_sql = ALTER_RMV_TEMPLATE.render(
        table_name="test_rmv",
        if_not_exists=if_not_exists,
        db=database_name,
        refresh_interval="EVERY 1 HOUR",
        depends_on=depends_on,
        # can't change select with alter
        # select_query="SELECT * FROM src1",
        with_append=with_append,
        on_cluster="test_cluster" if on_cluster else None,
        settings=settings,
    )

    node.query(alter_sql)
    show_create_after_alter = node.query(f"SHOW CREATE {maybe_db}test_rmv")
    assert show_create == show_create_after_alter
    compare_DDL_on_all_nodes()


def get_rmv_info(
    node, table, condition=None, max_attempts=50, delay=0.3, wait_status="Scheduled"
):
    def inner():
        rmv_info = node.query_with_retry(
            f"SELECT * FROM system.view_refreshes WHERE view='{table}'",
            check_callback=(lambda r: r.iloc[0]["status"] == wait_status)
            if wait_status
            else (lambda x: True),
            parse=True,
        ).to_dict("records")[0]

        # Is it time for python clickhouse-driver?
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


# Add cases for:
# Randomize bigger than interval
# executed even with empty
"AFTER 1 MINUTE RANDOMIZE FOR 3 YEAR",
"EVERY 1 MINUTE OFFSET 1 SECOND RANDOMIZE FOR 3 YEAR",

# special case
"EVERY 999 MINUTE"

# should be restricted:
# Two different units in EVERY
"EVERY 1 YEAR 0 MONTH",
"EVERY 1 YEAR 1 MONTH",
"EVERY 1 YEAR 2 MONTH",
# "EVERY 1 YEAR 3 MONTH",
# "EVERY 1 YEAR 4 MONTH",
# "EVERY 1 YEAR 8 MONTH",
# "EVERY 1 YEAR 9 MONTH",
# "EVERY 1 YEAR 10 MONTH",
# "EVERY 1 YEAR 11 MONTH",
"EVERY 1 YEAR 12 MONTH",
# "EVERY 1 YEAR 13 MONTH",
# "EVERY 1 DAY",

# should be restricted?
"EVERY 1 YEAR 2 YEAR",
"EVERY 1 MONTH 2 MONTH",
"AFTER 1 YEAR 2 YEAR",
"AFTER 1 MONTH 2 MONTH",

"EVERY 1 DAY 0 HOUR",
"EVERY 1 DAY 1 HOUR",
"EVERY 1 DAY 2 HOUR",
"EVERY 1 DAY 23 HOUR",
"EVERY 1 DAY 24 HOUR",
"EVERY 1 DAY 28 HOUR",
# "EVERY 1 DAY 29 HOUR",
# "EVERY 1 DAY 30 HOUR",
# "EVERY 1 DAY 31 HOUR",
"EVERY 1 DAY 32 HOUR",

# Interval shouldn't contain both calendar units and clock units (e.g. months and days)
# "EVERY 1 YEAR 10 MONTH 3 DAY 7 HOUR 5 MINUTE 30 SECOND",


INTERVALS_EVERY = [
    "EVERY 60 SECOND",
    "EVERY 1 MINUTE",
    "EVERY 1 HOUR",
    "EVERY 1 DAY",
    "EVERY 1 MONTH",
    "EVERY 1 YEAR",
    "EVERY 1 WEEK RANDOMIZE FOR 1 HOUR",
    "EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE",
    "EVERY 1 MONTH OFFSET 5 DAY 2 HOUR 30 MINUTE 15 SECOND",
    "EVERY 1 MONTH OFFSET 1 DAY RANDOMIZE FOR 10 HOUR",
    "EVERY 1 WEEK",
    "EVERY 2 WEEK",
    "EVERY 3 WEEK",
    "EVERY 4 WEEK",
    "EVERY 5 WEEK",
]

INTERVALS_AFTER = [
    "AFTER 59 SECOND",
    "AFTER 60 SECOND",
    "AFTER 61 SECOND",
    "AFTER 30 MINUTE",
    "AFTER 9999 MINUTE",
    "AFTER 2 HOUR",
    "AFTER 2 DAY",
    "AFTER 2 WEEK",
    "AFTER 2 MONTH",
    "AFTER 2 YEAR",
    "AFTER 2 MONTH 3 DAY",
    "AFTER 2 MONTH 3 DAY RANDOMIZE FOR 1 DAY",
    "AFTER 1 YEAR RANDOMIZE FOR 11 MONTH",
    "AFTER 1 MONTH",
    "AFTER 1 MONTH 0 DAY",
    "AFTER 1 MONTH 1 DAY",
    "AFTER 1 MONTH 3 DAY",
    "AFTER 1 MONTH 50 DAY",
    "AFTER 1 YEAR 10 MONTH",
    "AFTER 1 YEAR 10 MONTH 3 DAY",
    "AFTER 1 YEAR 10 MONTH 3 DAY 7 HOUR",
    "AFTER 1 YEAR 10 MONTH 3 DAY 7 HOUR 5 MINUTE 30 SECOND",
]


@pytest.fixture(scope="function")
def rmv_schedule_teardown():
    node.query("DROP TABLE IF EXISTS test_rmv")


def expect_rows(rows, table="test_rmv"):
    inserted_data = node.query_with_retry(
        f"SELECT * FROM {table}",
        parse=True,
        check_callback=lambda x: len(x) == rows,
        retry_count=100,
    )
    assert len(inserted_data) == rows


@pytest.mark.parametrize("interval", INTERVALS_AFTER + INTERVALS_EVERY)
@pytest.mark.parametrize(
    "append",
    [True, False],
)
@pytest.mark.parametrize(
    "empty",
    [True, False],
)
def test_rmv_scheduling(
    rmv_schedule_teardown,
    interval,
    append,
    empty,
):
    """
    Test creates RMV with multiple intervals, checks correctness of 'next_refresh_time' using schedule model,
    check that data correctly written, set fake time and check all again.
    """
    create_sql = CREATE_RMV_TEMPLATE.render(
        table_name="test_rmv",
        refresh_interval=interval,
        table_clause="ENGINE = Memory",
        select_query="SELECT now() as a, number as b FROM numbers(2)",
        with_append=append,
        empty=empty,
    )
    node.query(create_sql)

    now = datetime.utcnow()
    rmv = get_rmv_info(node, "test_rmv")
    predicted_next_refresh_time = get_next_refresh_time(interval, now, first_week=True)
    compare_dates(
        rmv["next_refresh_time"], predicted_next_refresh_time, first_week=True
    )
    assert rmv["next_refresh_time"] > now
    assert rmv["exception"] is None

    if empty:
        # No data is inserted with empty
        expect_rows(0)
    else:
        # Query is executed one time
        compare_dates_(rmv["last_success_time"], now)
        expect_rows(2)

    # TODO: SET FAKE TIME doesn't work for these
    if "RANDOMIZE" in interval:
        return
    if "OFFSET" in interval:
        return

    # Trigger refresh, update `next_refresh_time` and check interval again
    node.query(f"SYSTEM TEST VIEW test_rmv SET FAKE TIME '{rmv['next_refresh_time']}'")
    predicted_next_refresh_time2 = get_next_refresh_time(
        interval, rmv["next_refresh_time"]
    )
    rmv2 = get_rmv_info(
        node,
        "test_rmv",
        condition=lambda x: x["next_refresh_time"] != rmv["next_refresh_time"],
    )
    compare_dates(rmv2["next_refresh_time"], predicted_next_refresh_time2)
    assert rmv["exception"] is None, rmv

    if append and not empty:
        expect_rows(4)
    else:
        expect_rows(2)

    compare_dates(
        rmv2["last_success_time"], predicted_next_refresh_time, first_week=True
    )


@pytest.fixture(scope="function")
def fn2_setup_tables():
    node.query(
        f"CREATE TABLE tgt1 ON CLUSTER test_cluster (a DateTime, b UInt64) ENGINE = MergeTree ORDER BY tuple()"
    )

    node.query(
        f"CREATE TABLE src1 ON CLUSTER test_cluster (a DateTime, b UInt64) ENGINE = Memory"
    )
    node.query(f"INSERT INTO src1 VALUES ('2020-01-01', 1), ('2020-01-02', 2)")

    yield
    node.query("DROP TABLE IF EXISTS src1 ON CLUSTER test_cluster")
    node.query("DROP TABLE IF EXISTS tgt1 ON CLUSTER test_cluster")
    node.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER test_cluster")


@pytest.mark.parametrize(
    "append",
    [True, False],
)
# @pytest.mark.parametrize("on_cluster", [True, False])
@pytest.mark.parametrize(
    "empty",
    [True, False],
)
@pytest.mark.parametrize(
    "to_clause",
    [(None, "tgt1", "tgt1"), ("Engine MergeTree ORDER BY tuple()", None, "test_rmv")],
)
def test_real_wait_refresh(
    fn2_setup_tables,
    started_cluster,
    append,
    # on_cluster,
    empty,
    to_clause,
):
    table_clause, to_clause_, tgt = to_clause

    create_sql = CREATE_RMV_TEMPLATE.render(
        table_name="test_rmv",
        # if_not_exists=if_not_exists,
        refresh_interval="EVERY 8 SECOND",
        to_clause=to_clause_,
        table_clause=table_clause,
        select_query="SELECT a as a, b FROM src1",
        with_append=append,
        # on_cluster="test_cluster" if on_cluster else None,
        empty=empty
        # settings={'setting1':'value1', 'setting2': 'value2'},
    )
    node.query(create_sql)
    rmv = get_rmv_info(node, "test_rmv")

    append_expected_rows = 0
    if not empty:
        append_expected_rows += 2

    if empty:
        expect_rows(0, table=tgt)
    else:
        expect_rows(2, table=tgt)

    rmv2 = get_rmv_info(
        node,
        "test_rmv",
        condition=lambda x: x["last_refresh_time"] == rmv["next_refresh_time"],
        # wait for refresh a little bit more than 8 seconds
        max_attempts=10,
        delay=1,
    )

    if append:
        append_expected_rows += 2
        expect_rows(append_expected_rows, table=tgt)
    else:
        expect_rows(2, table=tgt)

    assert rmv2["exception"] is None
    assert rmv2["refresh_count"] == 2
    assert rmv2["status"] == "Scheduled"
    assert rmv2["last_refresh_result"] == "Finished"
    # assert rmv2["progress"] == , 1rmv2
    assert rmv2["last_success_time"] == rmv["next_refresh_time"]
    assert rmv2["last_refresh_time"] == rmv["next_refresh_time"]
    assert rmv2["retry"] == 0
    assert rmv2["read_rows"] == 2
    assert rmv2["read_bytes"] == 24
    assert rmv2["total_rows"] == 0
    assert rmv2["total_bytes"] == 0
    assert rmv2["written_rows"] == 0
    assert rmv2["written_bytes"] == 0
    assert rmv2["result_rows"] == 0
    assert rmv2["result_bytes"] == 0

    node.query("SYSTEM STOP VIEW test_rmv")
    time.sleep(10)
    rmv3 = get_rmv_info(node, "test_rmv")
    # no refresh happen
    assert rmv3["status"] == "Disabled"

    del rmv3["status"]
    del rmv2["status"]
    assert rmv3 == rmv2

    node.query("SYSTEM START VIEW test_rmv")
    time.sleep(1)
    rmv4 = get_rmv_info(node, "test_rmv")

    if append:
        append_expected_rows += 2
        expect_rows(append_expected_rows, table=tgt)
    else:
        expect_rows(2, table=tgt)

    assert rmv4["exception"] is None
    assert rmv4["refresh_count"] == 3
    assert rmv4["status"] == "Scheduled"
    assert rmv4["retry"] == 0
    assert rmv4["read_rows"] == 2
    assert rmv4["read_bytes"] == 24
    # why 0?
    assert rmv4["total_rows"] == 0
    assert rmv4["total_bytes"] == 0
    assert rmv4["written_rows"] == 0
    assert rmv4["written_bytes"] == 0
    assert rmv4["result_rows"] == 0
    assert rmv4["result_bytes"] == 0

    node.query("SYSTEM REFRESH VIEW test_rmv")
    time.sleep(1)
    if append:
        append_expected_rows += 2
        expect_rows(append_expected_rows, table=tgt)
    else:
        expect_rows(2, table=tgt)


@pytest.fixture(scope="function")
def fn3_setup_tables():
    node.query(
        f"CREATE TABLE tgt1 ON CLUSTER test_cluster (a DateTime, b UInt64) ENGINE = MergeTree ORDER BY tuple()"
    )
    node.query(
        f"CREATE TABLE src1 ON CLUSTER test_cluster (a DateTime, b UInt64) ENGINE = Memory"
    )
    node.query(f"INSERT INTO src1 SELECT toDateTime(1) a, 1 b FROM numbers(1000000000)")

    yield
    node.query("DROP TABLE IF EXISTS tgt1 ON CLUSTER test_cluster")
    node.query("DROP TABLE IF EXISTS src1 ON CLUSTER test_cluster")
    node.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER test_cluster")


@pytest.mark.parametrize(
    "select_query",
    ["SELECT toDateTime(1) a, 1 b FROM numbers(1000000000)", "SELECT * FROM src1"],
)
def test_long_query(fn3_setup_tables, select_query):
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    create_sql = CREATE_RMV_TEMPLATE.render(
        table_name="test_rmv",
        refresh_interval="EVERY 1 HOUR",
        to_clause="tgt1",
        select_query=select_query,
        with_append=False,
        empty=True,
    )
    node.query(create_sql)
    rmv = get_rmv_info(node, "test_rmv")
    node.query(f"SYSTEM TEST VIEW test_rmv SET FAKE TIME '{rmv['next_refresh_time']}'")

    wait_seconds = 0
    while wait_seconds < 30:
        time.sleep(1)
        wait_seconds += 1
        rmv = get_rmv_info(node, "test_rmv", wait_status=None)
        logging.info(rmv)
        if rmv["progress"] == 1:
            break

        assert rmv["status"] == "Running"
        assert 0 < rmv["progress"] < 1
        assert 0 < rmv["read_rows"] < 1000000000
        assert 0 < rmv["read_bytes"] < 8000000000

    assert rmv["progress"] == 1
    assert rmv["exception"] is None
    assert 3 <= wait_seconds <= 30, wait_seconds
    assert rmv["duration_ms"] >= 3000
    assert rmv["total_rows"] == 1000000000
    assert rmv["read_rows"] == 1000000000
    assert 0 < rmv["read_bytes"] == 8000000000


@pytest.mark.parametrize(
    "select_query",
    ["SELECT toDateTime(1) a, 1 b FROM numbers(1000000000)", "SELECT * FROM src1"],
)
def test_long_query_cancel(fn3_setup_tables, select_query):
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    create_sql = CREATE_RMV_TEMPLATE.render(
        table_name="test_rmv",
        refresh_interval="EVERY 1 HOUR",
        to_clause="tgt1",
        select_query=select_query,
        with_append=False,
        empty=True,
    )
    node.query(create_sql)
    rmv = get_rmv_info(node, "test_rmv")
    node.query(f"SYSTEM TEST VIEW test_rmv SET FAKE TIME '{rmv['next_refresh_time']}'")
    time.sleep(1)

    node.query("SYSTEM CANCEL VIEW test_rmv")
    time.sleep(1)
    rmv = get_rmv_info(node, "test_rmv", wait_status=None)
    assert rmv["status"] == "Scheduled"
    assert rmv["last_refresh_result"] == "Cancelled"
    assert 0 < rmv["progress"] < 1
    assert 0 < rmv["read_rows"] < 1000000000
    assert 0 < rmv["read_bytes"] < 8000000000

    assert rmv["last_success_time"] is None
    assert rmv["duration_ms"] > 0
    assert rmv["total_rows"] == 1000000000


@pytest.fixture(scope="function")
def fn4_setup_tables():
    node.query(
        f"CREATE TABLE tgt1 ON CLUSTER test_cluster (a DateTime) ENGINE = Memory"
    )
    yield
    node.query("DROP TABLE IF EXISTS tgt1 ON CLUSTER test_cluster")
    node.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER test_cluster")


def test_query_fail(fn4_setup_tables):
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    create_sql = CREATE_RMV_TEMPLATE.render(
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


def test_query_retry(fn4_setup_tables):
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    create_sql = CREATE_RMV_TEMPLATE.render(
        table_name="test_rmv",
        refresh_interval="EVERY 1 HOUR",
        to_clause="tgt1",
        select_query="SELECT throwIf(1, '111') a",  # exception in 4/5 calls
        with_append=False,
        empty=True,
        settings={
            "refresh_retries": "10",  # TODO -1
            "refresh_retry_initial_backoff_ms": "10",
            "refresh_retry_max_backoff_ms": "10",
        },
    )
    node.query(create_sql)
    rmv = get_rmv_info(node, "test_rmv")
    node.query(f"SYSTEM TEST VIEW test_rmv SET FAKE TIME '{rmv['next_refresh_time']}'")
    time.sleep(20)

    rmv2 = get_rmv_info(
        node,
        "test_rmv",
        wait_status=None,
    )
    assert rmv2["last_refresh_result"] == "Error"
    assert rmv2["retry"] == 10
    assert False


# def test_query_retry_success(fn4_setup_tables):
#     TODO: SELECT throwIf(rand() % 10 != 0, '111') a
#     pass


# TODO -1
