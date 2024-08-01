import datetime
import os
import random
import shutil
import time
import re
from typing import Optional

import numpy as np
import pytest
import threading

from dateutil.relativedelta import relativedelta
from jinja2 import Template, Environment
from datetime import datetime, timedelta

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, assert_logs_contain
from helpers.network import PartitionManager
from test_refreshable_mat_view.schedule_model import get_next_refresh_time

cluster = ClickHouseCluster(__file__)

node1_1 = cluster.add_instance(
    "node1_1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)
node1_2 = cluster.add_instance(
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
    print(node1_1.query("SELECT version()"))

    node1_1.query(f"CREATE DATABASE test_db ON CLUSTER test_cluster ENGINE = Atomic")

    node1_1.query(
        f"CREATE TABLE src1 ON CLUSTER test_cluster (a DateTime, b UInt64) ENGINE = Memory"
    )

    node1_1.query(
        f"CREATE TABLE src2 ON CLUSTER test_cluster (a DateTime, b UInt64) ENGINE = Memory"
    )

    node1_1.query(
        f"CREATE TABLE tgt1 ON CLUSTER test_cluster (a DateTime, b UInt64) ENGINE = Memory"
    )

    node1_1.query(
        f"CREATE TABLE tgt2 ON CLUSTER test_cluster (a DateTime, b UInt64) ENGINE = Memory"
    )

    node1_1.query(
        f"CREATE MATERIALIZED VIEW dummy_rmv ON CLUSTER test_cluster "
        f"REFRESH EVERY 10 HOUR engine Memory AS select number as x from numbers(10)"
    )


"""
#!/usr/bin/env bash
# Tags: atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Set session timezone to UTC to make all DateTime formatting and parsing use UTC, because refresh
# scheduling is done in UTC.
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT" | sed 's/--session_timezone[= ][^ ]*//g'`"
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT --allow_experimental_refreshable_materialized_view=1 --session_timezone Etc/UTC"`"

$CLICKHOUSE_CLIENT -nq "create view refreshes as select * from system.view_refreshes where database = '$CLICKHOUSE_DATABASE' order by view"


# Basic refreshing.
$CLICKHOUSE_CLIENT -nq "
    create materialized view a
        refresh after 2 second
        engine Memory
        empty
        as select number as x from numbers(2) union all select rand64() as x;
    select '<1: created view>', exception, view from refreshes;
    show create a;"
# Wait for any refresh. (xargs trims the string and turns \t and \n into spaces)
while [ "`$CLICKHOUSE_CLIENT -nq "select last_success_time is null from refreshes -- $LINENO" | xargs`" != '0' ]
do
    sleep 0.1
done
start_time="`$CLICKHOUSE_CLIENT -nq "select reinterpret(now64(), 'Int64')"`"
# Check table contents.
$CLICKHOUSE_CLIENT -nq "select '<2: refreshed>', count(), sum(x=0), sum(x=1) from a"
# Wait for table contents to change.
res1="`$CLICKHOUSE_CLIENT -nq 'select * from a order by x format Values'`"
while :
do
    res2="`$CLICKHOUSE_CLIENT -nq 'select * from a order by x format Values -- $LINENO'`"
    [ "$res2" == "$res1" ] || break
    sleep 0.1
done
# Wait for another change.
while :
do
    res3="`$CLICKHOUSE_CLIENT -nq 'select * from a order by x format Values -- $LINENO'`"
    [ "$res3" == "$res2" ] || break
    sleep 0.1
done
# Check that the two changes were at least 1 second apart, in particular that we're not refreshing
# like crazy. This is potentially flaky, but we need at least one test that uses non-mocked timer
# to make sure the clock+timer code works at all. If it turns out flaky, increase refresh period above.
$CLICKHOUSE_CLIENT -nq "
    select '<3: time difference at least>', min2(reinterpret(now64(), 'Int64') - $start_time, 1000);
    select '<4: next refresh in>', next_refresh_time-last_success_time from refreshes;"

# Create a source table from which views will read.
$CLICKHOUSE_CLIENT -nq "
    create table src (x Int8) engine Memory as select 1;"

# Switch to fake clock, change refresh schedule, change query.
$CLICKHOUSE_CLIENT -nq "
    system test view a set fake time '2050-01-01 00:00:01';
    system wait view a;
    system refresh view a;
    system wait view a;
    select '<4.1: fake clock>', status, last_success_time, next_refresh_time from refreshes;
    alter table a modify refresh every 2 year;
    alter table a modify query select x*2 as x from src;
    system wait view a;
    select '<4.5: altered>', status, last_success_time, next_refresh_time from refreshes;
    show create a;"
# Advance time to trigger the refresh.
$CLICKHOUSE_CLIENT -nq "
    select '<5: no refresh>', count() from a;
    system test view a set fake time '2052-02-03 04:05:06';"
while [ "`$CLICKHOUSE_CLIENT -nq "select last_success_time, status from refreshes -- $LINENO" | xargs`" != '2052-02-03 04:05:06 Scheduled' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    select '<6: refreshed>', * from a;
    select '<7: refreshed>', status, last_success_time, next_refresh_time from refreshes;"

# Create a dependent view, refresh it once.
$CLICKHOUSE_CLIENT -nq "
    create materialized view b refresh every 2 year depends on a (y Int32) engine MergeTree order by y empty as select x*10 as y from a;
    show create b;
    system test view b set fake time '2052-11-11 11:11:11';
    system refresh view b;
    system wait view b;
    select '<7.5: created dependent>', last_success_time from refreshes where view = 'b';"
# Next refresh shouldn't start until the dependency refreshes.
$CLICKHOUSE_CLIENT -nq "
    select '<8: refreshed>', * from b;
    select '<9: refreshed>', view, status, next_refresh_time from refreshes;
    system test view b set fake time '2054-01-24 23:22:21';"
while [ "`$CLICKHOUSE_CLIENT -nq "select status from refreshes where view = 'b' -- $LINENO" | xargs`" != 'WaitingForDependencies' ]
do
    sleep 0.1
done

# Drop the source table, check that refresh fails and doesn't leave a temp table behind.
$CLICKHOUSE_CLIENT -nq "
    select '<9.2: dropping>', countIf(name like '%tmp%'), countIf(name like '%.inner%') from system.tables where database = currentDatabase();
    drop table src;
    system refresh view a;"
$CLICKHOUSE_CLIENT -nq "system wait view a;" 2>/dev/null && echo "SYSTEM WAIT VIEW failed to fail at $LINENO"
$CLICKHOUSE_CLIENT -nq "
    select '<9.4: dropped>', countIf(name like '%tmp%'), countIf(name like '%.inner%') from system.tables where database = currentDatabase();"

# Create the source table again, check that refresh succeeds (in particular that tables are looked
# up by name rather than uuid).
$CLICKHOUSE_CLIENT -nq "
    select '<10: creating>', view, status, next_refresh_time from refreshes;
    create table src (x Int16) engine Memory as select 2;
    system test view a set fake time '2054-01-01 00:00:01';"
while [ "`$CLICKHOUSE_CLIENT -nq "select status from refreshes where view = 'b' -- $LINENO" | xargs`" != 'Scheduled' ]
do
    sleep 0.1
done
# Both tables should've refreshed.
$CLICKHOUSE_CLIENT -nq "
    select '<11: chain-refreshed a>', * from a;
    select '<12: chain-refreshed b>', * from b;
    select '<13: chain-refreshed>', view, status, last_success_time, last_refresh_time, next_refresh_time, exception == '' from refreshes;"

$CLICKHOUSE_CLIENT -nq "
    system test view b set fake time '2061-01-01 00:00:00';
    truncate src;
    insert into src values (3);
    system test view a set fake time '2060-02-02 02:02:02';"
while [ "`$CLICKHOUSE_CLIENT -nq "select next_refresh_time from refreshes where view = 'b' -- $LINENO" | xargs`" != '2062-01-01 00:00:00' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    select '<15: chain-refreshed a>', * from a;
    select '<16: chain-refreshed b>', * from b;
    select '<17: chain-refreshed>', view, status, next_refresh_time from refreshes;"

# Get to WaitingForDependencies state and remove the depencency.
$CLICKHOUSE_CLIENT -nq "
    system test view b set fake time '2062-03-03 03:03:03'"
while [ "`$CLICKHOUSE_CLIENT -nq "select status from refreshes where view = 'b' -- $LINENO" | xargs`" != 'WaitingForDependencies' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    alter table b modify refresh every 2 year"
while [ "`$CLICKHOUSE_CLIENT -nq "select status, last_refresh_time from refreshes where view = 'b' -- $LINENO" | xargs`" != 'Scheduled 2062-03-03 03:03:03' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    select '<18: removed dependency>', view, status, last_success_time, last_refresh_time, next_refresh_time from refreshes where view = 'b';
    show create b;"

# Select from a table that doesn't exist, get an exception.
$CLICKHOUSE_CLIENT -nq "
    drop table a;
    drop table b;
    create materialized view c refresh every 1 second (x Int64) engine Memory empty as select * from src;
    drop table src;"
while [ "`$CLICKHOUSE_CLIENT -nq "select exception == '' from refreshes -- $LINENO" | xargs`" != '0' ]
do
    sleep 0.1
done
# Check exception, create src, expect successful refresh.
$CLICKHOUSE_CLIENT -nq "
    select '<19: exception>', exception ilike '%UNKNOWN_TABLE%' from refreshes;
    create table src (x Int64) engine Memory as select 1;
    system refresh view c;
    system wait view c;"
# Rename table.
$CLICKHOUSE_CLIENT -nq "
    select '<20: unexception>', * from c;
    rename table c to d;
    select '<21: rename>', * from d;
    select '<22: rename>', view, status, last_success_time is null from refreshes;"

# Do various things during a refresh.
# First make a nonempty view.
$CLICKHOUSE_CLIENT -nq "
    drop table d;
    truncate src;
    insert into src values (1)
    create materialized view e refresh every 1 second (x Int64) engine MergeTree order by x as select x + sleepEachRow(1) as x from src settings max_block_size = 1;
    system wait view e;"
# Stop refreshes.
$CLICKHOUSE_CLIENT -nq "
    select '<23: simple refresh>', * from e;
    system stop view e;"
while [ "`$CLICKHOUSE_CLIENT -nq "select status from refreshes -- $LINENO" | xargs`" != 'Disabled' ]
do
    sleep 0.1
done
# Make refreshes slow, wait for a slow refresh to start. (We stopped refreshes first to make sure
# we wait for a slow refresh, not a previous fast one.)
$CLICKHOUSE_CLIENT -nq "
    insert into src select * from numbers(1000) settings max_block_size=1;
    system start view e;"
while [ "`$CLICKHOUSE_CLIENT -nq "select status from refreshes -- $LINENO" | xargs`" != 'Running' ]
do
    sleep 0.1
done
# Rename.
$CLICKHOUSE_CLIENT -nq "
    rename table e to f;
    select '<24: rename during refresh>', * from f;
    select '<25: rename during refresh>', view, status from refreshes;
    alter table f modify refresh after 10 year settings refresh_retries = 0;"
sleep 2 # make it likely that at least one row was processed
# Cancel.
$CLICKHOUSE_CLIENT -nq "
    system cancel view f;"
while [ "`$CLICKHOUSE_CLIENT -nq "select status from refreshes -- $LINENO" | xargs`" != 'Scheduled' ]
do
    sleep 0.1
done
# Check that another refresh doesn't immediately start after the cancelled one.
sleep 1
$CLICKHOUSE_CLIENT -nq "
    select '<27: cancelled>', view, status, exception from refreshes;
    system refresh view f;"
while [ "`$CLICKHOUSE_CLIENT -nq "select status from refreshes -- $LINENO" | xargs`" != 'Running' ]
do
    sleep 0.1
done
# Drop.
$CLICKHOUSE_CLIENT -nq "
    drop table f;
    select '<28: drop during refresh>', view, status from refreshes;
    select '<28: drop during refresh>', countIf(name like '%tmp%'), countIf(name like '%.inner%') from system.tables where database = currentDatabase()"

# Try OFFSET and RANDOMIZE FOR.
$CLICKHOUSE_CLIENT -nq "
    create materialized view g refresh every 1 week offset 3 day 4 hour randomize for 4 day 1 hour (x Int64) engine Memory empty as select 42 as x;
    show create g;
    system test view g set fake time '2050-02-03 15:30:13';"
while [ "`$CLICKHOUSE_CLIENT -nq "select next_refresh_time > '2049-01-01' from refreshes -- $LINENO" | xargs`" != '1' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    with '2050-02-10 04:00:00'::DateTime as expected
    select '<29: randomize>', abs(next_refresh_time::Int64 - expected::Int64) <= 3600*(24*4+1), next_refresh_time != expected from refreshes;"

# Send data 'TO' an existing table.
$CLICKHOUSE_CLIENT -nq "
    drop table g;
    create table dest (x Int64) engine MergeTree order by x;
    truncate src;
    insert into src values (1);
    create materialized view h refresh every 1 second to dest as select x*10 as x from src;
    show create h;
    system wait view h;
    select '<30: to existing table>', * from dest;
    insert into src values (2);"
while [ "`$CLICKHOUSE_CLIENT -nq "select count() from dest -- $LINENO" | xargs`" != '2' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    select '<31: to existing table>', * from dest;
    drop table dest;
    drop table h;"

# Retries.
$CLICKHOUSE_CLIENT -nq "
    create materialized view h2 refresh after 1 year settings refresh_retries = 10 (x Int64) engine Memory as select x*10 + throwIf(x % 2 == 0) as x from src;"
$CLICKHOUSE_CLIENT -nq "system wait view h2;" 2>/dev/null && echo "SYSTEM WAIT VIEW failed to fail at $LINENO"
$CLICKHOUSE_CLIENT -nq "
    select '<31.5: will retry>', exception != '', retry > 0 from refreshes;
    create table src2 empty as src;
    insert into src2 values (1)
    exchange tables src and src2;
    drop table src2;"
while [ "`$CLICKHOUSE_CLIENT -nq "select status, retry from refreshes -- $LINENO" | xargs`" != 'Scheduled 0' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    select '<31.6: did retry>', x from h2;
    drop table h2"

# EMPTY
$CLICKHOUSE_CLIENT -nq "
    create materialized view i refresh after 1 year engine Memory empty as select number as x from numbers(2);
    system wait view i;
    create materialized view j refresh after 1 year engine Memory as select number as x from numbers(2);"
while [ "`$CLICKHOUSE_CLIENT -nq "select sum(last_success_time is null) from refreshes -- $LINENO" | xargs`" == '2' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    select '<32: empty>', view, status, last_success_time is null, retry from refreshes order by view;
    drop table i;
    drop table j;"

# APPEND
$CLICKHOUSE_CLIENT -nq "
    create materialized view k refresh every 10 year append (x Int64) engine Memory empty as select x*10 as x from src;
    select '<33: append>', * from k;
    system refresh view k;
    system wait view k;
    select '<34: append>', * from k;
    truncate table src;
    insert into src values (2), (3);
    system refresh view k;
    system wait view k;
    select '<35: append>', * from k order by x;"
# ALTER to non-APPEND
$CLICKHOUSE_CLIENT -nq "
    alter table k modify refresh every 10 year;" 2>/dev/null || echo "ALTER from APPEND to non-APPEND failed, as expected"
$CLICKHOUSE_CLIENT -nq "
    drop table k;
    truncate table src;"

# APPEND + TO + regular materialized view reading from it.
$CLICKHOUSE_CLIENT -nq "
    create table mid (x Int64) engine MergeTree order by x;
    create materialized view l refresh every 10 year append to mid empty as select x*10 as x from src;
    create materialized view m (x Int64) engine Memory as select x*10 as x from mid;
    insert into src values (1);
    system refresh view l;
    system wait view l;
    select '<37: append chain>', * from m;
    insert into src values (2);
    system refresh view l;
    system wait view l;
    select '<38: append chain>', * from m order by x;
    drop table l;
    drop table m;
    drop table mid;"

# Failing to create inner table.
$CLICKHOUSE_CLIENT -nq "
    create materialized view n refresh every 1 second (x Int64) engine MergeTree as select 1 as x from numbers(2);" 2>/dev/null || echo "creating MergeTree without ORDER BY failed, as expected"
$CLICKHOUSE_CLIENT -nq "
    create materialized view n refresh every 1 second (x Int64) engine MergeTree order by x as select 1 as x from numbers(2);
    drop table n;"

$CLICKHOUSE_CLIENT -nq "
    drop table refreshes;"

Information about Refreshable Materialized Views. Contains all refreshable materialized views, regardless of whether there's a refresh in progress or not.

Columns:

database (String) — The name of the database the table is in.
view (String) — Table name.
status (String) — Current state of the refresh.
last_success_time (Nullable(DateTime)) — Time when the latest successful refresh started. NULL if no successful refreshes happened since server startup or table creation.
last_success_time (Nullable(UInt64)) — How long the latest refresh took.
last_refresh_time (Nullable(DateTime)) — Time when the latest refresh attempt finished (if known) or started (if unknown or still running). NULL if no refresh attempts happened since server startup or table creation.
last_refresh_replica (String) — If coordination is enabled, name of the replica that made the current (if running) or previous (if not running) refresh attempt.
next_refresh_time (Nullable(DateTime)) — Time at which the next refresh is scheduled to start, if status = Scheduled.
exception (String) — Error message from previous attempt if it failed.
retry (UInt64) — How many failed attempts there were so far, for the current refresh.
progress (Float64) — Progress of the current refresh, between 0 and 1. Not available if status is RunningOnAnotherReplica.
read_rows (UInt64) — Number of rows read by the current refresh so far. Not available if status is RunningOnAnotherReplica.
total_rows (UInt64) — Estimated total number of rows that need to be read by the current refresh. Not available if status is RunningOnAnotherReplica.

Available refresh settings:
 * `refresh_retries` - How many times to retry if refresh query fails with an exception. If all retries fail, skip to the next scheduled refresh time. 0 means no retries, -1 means infinite retries. Default: 0.
 * `refresh_retry_initial_backoff_ms` - Delay before the first retry, if `refresh_retries` is not zero. Each subsequent retry doubles the delay, up to `refresh_retry_max_backoff_ms`. Default: 100 ms.
 * `refresh_retry_max_backoff_ms` - Limit on the exponential growth of delay between refresh attempts. Default: 60000 ms (1 minute).



CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name
REFRESH EVERY|AFTER interval [OFFSET interval]
RANDOMIZE FOR interval
DEPENDS ON [db.]name [, [db.]name [, ...]]
SETTINGS name = value [, name = value [, ...]]
[APPEND]
[TO[db.]name] [(columns)] [ENGINE = engine] [EMPTY]
AS SELECT ...
where interval is a sequence of simple intervals:

number SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR

Example refresh schedules:
```sql
REFRESH EVERY 1 DAY -- every day, at midnight (UTC)
REFRESH EVERY 1 MONTH -- on 1st day of every month, at midnight
REFRESH EVERY 1 MONTH OFFSET 5 DAY 2 HOUR -- on 6th day of every month, at 2:00 am
REFRESH EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE -- every other Saturday, at 3:10 pm
REFRESH EVERY 30 MINUTE -- at 00:00, 00:30, 01:00, 01:30, etc
REFRESH AFTER 30 MINUTE -- 30 minutes after the previous refresh completes, no alignment with time of day
-- REFRESH AFTER 1 HOUR OFFSET 1 MINUTE -- syntax errror, OFFSET is not allowed with AFTER
```

`RANDOMIZE FOR` randomly adjusts the time of each refresh, e.g.:
```sql
REFRESH EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 1 HOUR -- every day at random time between 01:30 and 02:30


SYSTEM REFRESH VIEW

Wait for the currently running refresh to complete. If the refresh fails, throws an exception. If no refresh is running, completes immediately, throwing an exception if previous refresh failed.

### SYSTEM STOP VIEW, SYSTEM STOP VIEWS

### SYSTEM WAIT VIEW

Waits for the running refresh to complete. If no refresh is running, returns immediately. If the latest refresh attempt failed, reports an error.

Can be used right after creating a new refreshable materialized view (without EMPTY keyword) to wait for the initial refresh to complete.

```sql
SYSTEM WAIT VIEW [db.]name

### TESTS
1. ON CLUSTER?
2. Append mode
3. Drop node and wait for restore
4. Simple functional testing: all values in refresh result correct (two and more rmv)
5. Combinations of time
6. Two (and more) rmv from single to single [APPEND]
7. ALTER rmv  ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
8. RMV without tgt table (automatic table) (check APPEND)

8 DROP rmv
9 CREATE - DROP - CREATE - ALTER
10 Backups?
11. Long queries over refresh time (check settings)
12. RMV settings
13. incorrect intervals (interval 1 sec, offset 1 minute)
14. ALTER on cluster

15. write to distributed with / without APPEND
16. `default_replica_path` empty on database replicated
17. Not existent TO table (ON CLUSTER)
18. Not existent FROM table (ON CLUSTER)
19. Not existent BOTH tables (ON CLUSTER)
20. retry failed
21. overflow with wait test

22. ALTER of SELECT?

23. OFFSET must be less than the period. 'EVERY 1 MONTH OFFSET 5 WEEK'



"""


def sql_template(
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


CREATE_RMV_TEMPLATE = sql_template(
    """CREATE MATERIALIZED VIEW
{% if if_not_exists %}IF NOT EXISTS{% endif %}
{% if db %}{{db}}.{% endif %}{{ table_name }}
{% if on_cluster %}ON CLUSTER {{ on_cluster }}{% endif %}
REFRESH {{ refresh_interval }}
{% if depends_on %}DEPENDS ON {{ depends_on|join(', ') }}{% endif %}
{% if settings %}SETTINGS {{ settings|format_settings }}{% endif %}
{% if with_append %}APPEND{% endif %}
{% if to_clause %}TO {{ to_clause }}{% endif %}
{% if table_clause %}{{ table_clause }}{% endif %}
{% if empty %}EMPTY{% endif %}
AS {{ select_query }}"""
)

# ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]

ALTER_RMV_TEMPLATE = sql_template(
    """ALTER TABLE
{% if db %}{{db}}.{% endif %}{{ table_name }}
{% if on_cluster %}ON CLUSTER {{ on_cluster }}{% endif %}
MODIFY REFRESH {{ refresh_interval }}
{% if depends_on %}DEPENDS ON {{ depends_on|join(', ') }}{% endif %}
{% if with_append %}APPEND{% endif %}
{% if settings %}SETTINGS {{ settings|format_settings }}{% endif %}"""
)


@pytest.mark.parametrize("with_append", [True, False])
@pytest.mark.parametrize("create_target_table", [True, False])
@pytest.mark.parametrize("if_not_exists", [True, False])
@pytest.mark.parametrize("on_cluster", [True, False])
@pytest.mark.parametrize(
    "depends_on", [None, ["dummy_rmv"], ["default.dummy_rmv", "src1"]]
)
@pytest.mark.parametrize("empty", [True, False])
@pytest.mark.parametrize("database_name", [None, "default", "test_db"])
def test_correct_states(
    request,
    started_cluster,
    with_append,
    create_target_table,
    if_not_exists,
    on_cluster,
    depends_on,
    empty,
    database_name,
):
    """
    Check correctness of functional states of RMV after CREATE, DROP, ALTER, trigger of RMV, ...
    Check ref
    """

    def teardown():
        node1_1.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER test_cluster")

    request.addfinalizer(teardown)

    create_sql = CREATE_RMV_TEMPLATE.render(
        table_name="test_rmv",
        if_not_exists=if_not_exists,
        db_name=database_name,
        refresh_interval="EVERY 1 HOUR",
        depends_on=depends_on,
        to_clause="tgt1",
        select_query="SELECT * FROM src1",
        with_append=with_append,
        on_cluster="test_cluster" if on_cluster else None,
        # settings={'setting1':'value1', 'setting2': 'value2'},
    )
    print(create_sql)
    node1_1.query(create_sql)
    refreshes = node1_1.query(
        "SELECT hostname(), * FROM clusterAllReplicas('test_cluster', system.view_refreshes)",
        parse=True,
    )

    # Check same RMV is created on cluster
    def compare_create_all_nodes():
        show_create_all_nodes = cluster.query_all_nodes("SHOW CREATE test_rmv")

        if not on_cluster:
            del show_create_all_nodes["node1_1"]

        assert_same_values(show_create_all_nodes.values())

    compare_create_all_nodes()

    show_create = node1_1.query("SHOW CREATE test_rmv")

    # Alter of RMV replaces all non-specified
    alter_sql = ALTER_RMV_TEMPLATE.render(
        table_name="test_rmv",
        if_not_exists=if_not_exists,
        db_name=database_name,
        refresh_interval="EVERY 1 HOUR",
        depends_on=depends_on,
        select_query="SELECT * FROM src1",
        with_append=with_append,
        on_cluster="test_cluster" if on_cluster else None,
        # settings={'setting1':'value1', 'setting2': 'value2'},
    )

    node1_1.query(alter_sql)
    show_create_after_alter = node1_1.query("SHOW CREATE test_rmv")

    compare_create_all_nodes()
    assert show_create == show_create_after_alter
    # breakpoint()


def compare_dates(
    date1: str | datetime,
    date2: str | datetime | tuple[datetime],
    inaccuracy=timedelta(hours=1),
    format_str="%Y-%m-%d %H:%M:%S",
) -> bool:
    """
    Compares two dates with an inaccuracy of 2 minutes.
    """
    if isinstance(date2, tuple):
        return date2[0] <= date1 <= date2[1]

    if isinstance(date1, str):
        date1 = datetime.strptime(date1, format_str)
    if isinstance(date2, str):
        date2 = datetime.strptime(date2, format_str)

    return abs(date1 - date2) <= inaccuracy


def date_in_interval(
    date1: str | datetime,
    date2: str | datetime,
    inaccuracy=timedelta(minutes=2),
    format_str="%Y-%m-%d %H:%M:%S",
):
    pass


def get_rmv_info(node, table):
    rmv_info = node.query_with_retry(
        f"SELECT * FROM system.view_refreshes WHERE view='{table}'",
        check_callback=lambda r: r.iloc[0]["status"] == "Scheduled",
        parse=True,
    ).to_dict("records")[0]

    rmv_info["next_refresh_time"] = parse_ch_datetime(rmv_info["next_refresh_time"])

    return rmv_info



@pytest.mark.parametrize("with_append", [True, False])
@pytest.mark.parametrize("create_target_table", [True, False])
# @pytest.mark.parametrize("if_not_exists", [True, False])
@pytest.mark.parametrize("on_cluster", [True, False])
@pytest.mark.parametrize("depends_on", [None, ["dummy_rmv"]])
@pytest.mark.parametrize("randomize", [True, False])
@pytest.mark.parametrize("empty", [True, False])
@pytest.mark.parametrize("database_name", [None, "default", "test_db"])
def test_check_data(
    request,
    started_cluster,
    with_append,
    create_target_table,
    # if_not_exists,
    on_cluster,
    depends_on,
    randomize,
    empty,
    database_name,
):
    def teardown():
        node1_1.query("DROP TABLE IF EXISTS test_rmv ON CLUSTER test_cluster")
        node1_1.query("DROP TABLE IF EXISTS tgt_new ON CLUSTER test_cluster")
        node1_1.query("TRUNCATE TABLE tgt1 ON CLUSTER test_cluster")

    request.addfinalizer(teardown)

    # CREATE RMV can use already existed table
    # or create new one
    if create_target_table:
        to_clause = "tgt1"
        tgt = "tgt1"
    else:
        to_clause = "tgt_new Engine = Memory"
        tgt = "tgt_new"

    create_sql = CREATE_RMV_TEMPLATE.render(
        table_name="test_rmv",
        # if_not_exists=if_not_exists,
        db_name=database_name,
        refresh_interval="EVERY 1 HOUR",
        randomize_interval="30 MINUTE" if randomize else None,
        depends_on=depends_on,
        to_clause=to_clause,
        select_query="SELECT now() as a, number as b FROM numbers(2)",
        with_append=with_append,
        on_cluster="test_cluster" if on_cluster else None,
        empty=empty
        # settings={'setting1':'value1', 'setting2': 'value2'},
    )
    print(create_sql)
    node1_1.query(create_sql)

    # now = node1_1.query("SELECT now()", parse=True)[0][0]
    now = datetime.utcnow()

    rmv = get_rmv_info(node1_1, "test_rmv")
    print(rmv)

    """
    1. check table view_refreshes
    2. check inserted data if without EMPTY
    3. set time, wait for refresh
    4. check data inserted
    5. alter table
    """

    assert rmv["exception"] is None
    assert rmv["status"] == "Scheduled"
    assert rmv["progress"] is None

    if not empty:
        # Insert
        assert compare_dates(
            rmv["last_refresh_time"], now, format_str="%Y-%m-%d %H:%M:%S"
        )
        assert rmv["last_success_time"] > 0
        inserted_data = node1_1.query(f"SELECT * FROM {tgt}", parse=True)
        print(inserted_data)
        assert len(inserted_data) == 2

    if empty:
        assert rmv["last_refresh_time"] is None
        assert rmv["last_success_time"] is None

        assert rmv["retry"] == 0
        assert rmv["read_rows"] == 0
        assert rmv["read_bytes"] == 0
        assert rmv["total_rows"] == 0
        assert rmv["total_bytes"] == 0
        assert rmv["written_rows"] == 0
        assert rmv["written_bytes"] == 0
        assert rmv["result_rows"] == 0
        assert rmv["result_bytes"] == 0

    # Rewind to the next trigger and wait for it
    node1_1.query(
        f"SYSTEM TEST VIEW test_rmv set fake time '{rmv['next_refresh_time']}';"
    )

    now = datetime.utcnow()
    inserted_data = node1_1.query(f"SELECT * FROM {tgt}", parse=True)
    if with_append:
        assert len(inserted_data) == 4
    else:
        assert len(inserted_data) == 2

    # alter time


"""

(Pdb) pp interval
'EVERY 1 WEEK'
(Pdb) pp next_refresh_time
datetime.datetime(2024, 5, 6, 0, 0)
(Pdb) pp predicted_next_refresh_time
datetime.datetime(2024, 5, 6, 0, 0)


(Pdb) pp interval
'EVERY 2 WEEK'
(Pdb) pp next_refresh_time
datetime.datetime(2024, 5, 6, 0, 0)
(Pdb) pp predicted_next_refresh_time
datetime.datetime(2024, 5, 13, 0, 0)


(Pdb) pp interval
'EVERY 2 WEEK OFFSET 1 DAY'
(Pdb) pp next_refresh_time
datetime.datetime(2024, 5, 7, 0, 0)
(Pdb) pp predicted_next_refresh_time
datetime.datetime(2024, 5, 14, 0, 0)


(Pdb) pp interval
'EVERY 2 WEEK OFFSET 2 DAY'
(Pdb) pp next_refresh_time
datetime.datetime(2024, 5, 8, 0, 0)
(Pdb) pp predicted_next_refresh_time
datetime.datetime(2024, 5, 15, 0, 0)


'EVERY 1 WEEK OFFSET 2 DAY'
(Pdb) pp next_refresh_time
datetime.datetime(2024, 5, 1, 0, 0)
(Pdb) pp predicted_next_refresh_time
datetime.datetime(2024, 5, 8, 0, 0)

"""


def parse_ch_datetime(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")


INTERVALS_EVERY = [
    # Same units
    "EVERY 1 YEAR 2 YEAR",
    #
    "EVERY 1 MINUTE",
    "EVERY 1 HOUR",
    "EVERY 1 DAY",
    "EVERY 1 WEEK",
    "EVERY 2 WEEK",
    "EVERY 1 MONTH",
    "EVERY 1 YEAR",
    "EVERY 1 WEEK RANDOMIZE FOR 1 HOUR",
    "EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE",
    "EVERY 1 MONTH OFFSET 5 DAY 2 HOUR 30 MINUTE 15 SECOND",
    "EVERY 1 MONTH OFFSET 1 DAY RANDOMIZE FOR 10 HOUR",
    # smth wrong with
    # "EVERY 1 WEEK",
    # "EVERY 2 WEEK",
    # "EVERY 3 WEEK",
    # "EVERY 4 WEEK",
    # "EVERY 5 WEEK",
    # "EVERY 1 MONTH OFFSET 1 WEEK",
    # "EVERY 1 MONTH OFFSET 2 WEEK",
    # Two different units in EVERY
    # "EVERY 1 YEAR",
    # "EVERY 1 YEAR 0 MONTH",
    # "EVERY 1 YEAR 1 MONTH",
    # "EVERY 1 YEAR 2 MONTH",
    # "EVERY 1 YEAR 3 MONTH",
    # "EVERY 1 YEAR 4 MONTH",
    # "EVERY 1 YEAR 8 MONTH",
    # "EVERY 1 YEAR 9 MONTH",
    # "EVERY 1 YEAR 10 MONTH",
    # "EVERY 1 YEAR 11 MONTH",
    # "EVERY 1 YEAR 12 MONTH",
    # "EVERY 1 YEAR 13 MONTH",
    # "EVERY 1 DAY",
    # "EVERY 1 DAY 0 HOUR",
    # "EVERY 1 DAY 1 HOUR",
    # "EVERY 1 DAY 2 HOUR",
    # "EVERY 1 DAY 28 HOUR",
    # "EVERY 1 DAY 29 HOUR",
    # "EVERY 1 DAY 30 HOUR",
    # "EVERY 1 DAY 31 HOUR",
    # "EVERY 1 DAY 32 HOUR",
    # Interval shouldn't contain both calendar units and clock units (e.g. months and days)
    # "EVERY 1 YEAR 10 MONTH 3 DAY 7 HOUR 5 MINUTE 30 SECOND",
]

INTERVALS_AFTER = [
    "AFTER 1 YEAR 2 YEAR",
    "AFTER 30 SECOND",
    "AFTER 30 MINUTE",
    "AFTER 2 HOUR",
    "AFTER 2 DAY",
    "AFTER 2 WEEK",
    "AFTER 2 MONTH",
    "AFTER 2 YEAR",
    "AFTER 2 MONTH 3 DAY",
    "AFTER 2 MONTH 3 DAY RANDOMIZE FOR 1 DAY",
    "AFTER 1 YEAR RANDOMIZE FOR 11 MONTH",
    # Randomize bigger than interval
    "AFTER 1 MINUTE RANDOMIZE FOR 3 YEAR",
    "EVERY 1 MINUTE RANDOMIZE FOR 3 YEAR",
    "EVERY 1 MINUTE OFFSET 1 SECOND RANDOMIZE FOR 3 YEAR",
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


@pytest.mark.parametrize(
    "interval",
    INTERVALS_EVERY,
)
@pytest.mark.parametrize(
    "append",
    [
        # True,
        False
    ],
)
@pytest.mark.parametrize(
    "empty",
    [
        True,
        # False
    ],
)
def test_schedule_2(
    request,
    started_cluster,
    interval,
    append,
    empty,
):
    """
    - Create RMV
    - Check table view_refreshes
    - Check inserted data if without EMPTY
    - Set time, wait for refresh
    - Check data is inserted/appended
    - Alter table
    - Check everything again
    - DROP target table
    """

    # if "AFTER" in interval:
    #     pytest.skip()

    def teardown():
        node1_1.query("DROP TABLE IF EXISTS test_rmv_schedule")
        node1_1.query("DROP TABLE IF EXISTS tgt_new")
        node1_1.query("TRUNCATE TABLE tgt1")

    request.addfinalizer(teardown)

    create_sql = CREATE_RMV_TEMPLATE.render(
        table_name="test_rmv_schedule",
        refresh_interval=interval,
        table_clause="ENGINE = Memory",
        select_query="SELECT now() as a, number as b FROM numbers(2)",
        with_append=append,
        empty=empty,
    )
    print(create_sql)
    node1_1.query(create_sql)

    now = datetime.utcnow()

    rmv = get_rmv_info(node1_1, "test_rmv_schedule")
    # this is for EVERY
    predicted_next_refresh_time = get_next_refresh_time(interval, now)
    # this is for AFTER
    # diff = next_refresh_time - now.replace(microsecond=0)

    if 'WEEK' in
    assert compare_dates(rmv["next_refresh_time"], predicted_next_refresh_time)
    assert rmv["next_refresh_time"] > now

    def expect_rows(rows):
        inserted_data = node1_1.query_with_retry(
            f"SELECT * FROM test_rmv_schedule",
            parse=True,
            check_callback=lambda x: len(x) == rows,
            retry_count=100,
        )
        assert len(inserted_data) == rows

    # Check data if not EMPTY
    append_expect_rows = 0
    if append:
        # Append adds rows
        append_expect_rows += 2
        expect_rows(append_expect_rows)
    if not append:
        # Rewrite without append
        expect_rows(2)

    inserted_data = node1_1.query(f"SELECT * FROM test_rmv_schedule", parse=True)
    if empty:
        # No data is inserted with empty
        assert len(inserted_data) == 0
    else:
        # Query is executed without empty
        assert rmv["last_success_time"] > 0
        expect_rows(2)

    # Trigger refresh, update `next_refresh_time` and check interval again
    node1_1.query(
        f"SYSTEM TEST VIEW test_rmv_schedule SET FAKE TIME '{rmv['next_refresh_time']}'"
    )
    predicted_next_refresh_time = get_next_refresh_time(interval, rmv['next_refresh_time'])
    rmv = get_rmv_info(node1_1, "test_rmv_schedule")

    assert compare_dates(rmv["next_refresh_time"], predicted_next_refresh_time)

    # Check data if not EMPTY
    if append:
        # Append adds rows
        append_expect_rows += 2
        expect_rows(append_expect_rows)
    else:
        # Rewrite
        expect_rows(2)

    # breakpoint()


@pytest.mark.parametrize(
    "interval",
    INTERVALS_EVERY,
)
@pytest.mark.parametrize(
    "append",
    [
        # True,
        False
    ],
)
@pytest.mark.parametrize(
    "empty",
    [
        True,
        # False
    ],
)
def test_schedule(
    request,
    started_cluster,
    interval,
    append,
    empty,
):
    """
    - Create RMV
    - Check table view_refreshes
    - Check inserted data if without EMPTY
    - Set time, wait for refresh
    - Check data is inserted/appended
    - Alter table
    - Check everything again
    - DROP target table
    """

    # if "WEEK" in interval:
    #     pytest.skip()

    def teardown():
        node1_1.query("DROP TABLE IF EXISTS test_rmv_schedule")
        node1_1.query("DROP TABLE IF EXISTS tgt_new")
        node1_1.query("TRUNCATE TABLE tgt1")

    request.addfinalizer(teardown)

    create_sql = CREATE_RMV_TEMPLATE.render(
        table_name="test_rmv_schedule",
        refresh_interval=interval,
        table_clause="ENGINE = Memory",
        select_query="SELECT now() as a, number as b FROM numbers(2)",
        with_append=append,
        empty=empty,
    )
    print(create_sql)
    node1_1.query(create_sql)

    now = datetime.utcnow()

    rmv = get_rmv_info(node1_1, "test_rmv_schedule")
    print(rmv)

    next_refresh_time = parse_ch_datetime(rmv["next_refresh_time"])
    predicted_next_refresh_time = get_next_refresh_time(interval, now)

    print("----")
    print("current_time:", now)
    print("Interval:", interval)
    print("next_refresh_time", next_refresh_time)
    print("predicted", predicted_next_refresh_time)
    print("----")

    # RANDOMIZE means the next refresh time will be randomly chosen
    # within a range of RANDOMIZE interval
    if "RANDOMIZE" in interval:
        assert (
            predicted_next_refresh_time[0]
            <= next_refresh_time
            <= predicted_next_refresh_time[1]
        )
    else:
        assert compare_dates(next_refresh_time, predicted_next_refresh_time)

    assert next_refresh_time > now

    # append_expect_rows = 0
    #
    # def check_data():
    #     inserted_data = node1_1.query_with_retry(
    #         f"SELECT * FROM test_rmv_schedule",
    #         parse=True,
    #         check_callback=lambda x: len(x) > 0,
    #         retry_count=200,
    #     )
    #
    #     # Check data if not EMPTY
    #     if append:
    #         # Append adds rows
    #         global append_expect_rows
    #         append_expect_rows += 2
    #         assert len(inserted_data) == append_expect_rows
    #     if not append:
    #         # Rewrite without append
    #         assert len(inserted_data) == 2
    #
    # inserted_data = node1_1.query(f"SELECT * FROM test_rmv_schedule", parse=True)
    # if empty:
    #     assert len(inserted_data) == 0
    # else:
    #     assert rmv["last_success_time"] > 0
    #     check_data()
    #
    # # Trigger next refresh
    # node1_1.query(
    #     f"SYSTEM TEST VIEW test_rmv_schedule SET FAKE TIME '{next_refresh_time}'"
    # )
    #
    # if "RANDOMIZE" not in interval:
    #     check_data()
    # -----------------------------
    # rmv = get_rmv_info(node1_1, "test_rmv_schedule")
    # next_refresh_time = parse_ch_datetime(rmv["next_refresh_time"])

    # # Alter RMV to random interval and test schedule is changed
    # # TODO: remove week filter after fix
    # interval_alter = random.choice(list(filter(lambda x: "WEEK" not in x, INTERVALS)))
    # alter_sql = ALTER_RMV_TEMPLATE.render(
    #     table_name="test_rmv_schedule",
    #     refresh_interval=interval_alter,
    #     # select_query="SELECT * FROM src1",
    # )
    # print(alter_sql)
    # node1_1.query(alter_sql)
    # now_alter = datetime.utcnow()
    #
    # rmv = get_rmv_info(node1_1, "test_rmv_schedule")
    # next_refresh_time_alter = parse_ch_datetime(rmv["next_refresh_time"])
    # predicted_next_refresh_time = get_next_refresh_time(interval_alter, now_alter)
    #
    # if "RANDOMIZE" in interval_alter:
    #     assert (
    #         predicted_next_refresh_time[0]
    #         <= next_refresh_time_alter
    #         <= predicted_next_refresh_time[1]
    #     )
    # else:
    #     assert compare_dates(next_refresh_time_alter, predicted_next_refresh_time)
    #
    # assert next_refresh_time_alter > now_alter
    #
    # # Trigger next refresh
    # node1_1.query(
    #     f"SYSTEM TEST VIEW test_rmv_schedule SET FAKE TIME '{next_refresh_time_alter}'"
    # )
    # check_data()

    # breakpoint()
