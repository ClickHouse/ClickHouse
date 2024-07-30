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

test_recover_staled_replica_run = 1

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
node2_1 = cluster.add_instance(
    "node2_1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 2, "replica": 1},
)

node2_2 = cluster.add_instance(
    "node2_2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 2, "replica": 2},
)

# all_nodes = [
#     main_node,
#     dummy_node,
#     competing_node,
# ]

uuid_regex = re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def assert_create_query(nodes, table_name, expected):
    replace_uuid = lambda x: re.sub(uuid_regex, "uuid", x)
    query = "show create table {}".format(table_name)
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
last_success_duration_ms (Nullable(UInt64)) — How long the latest refresh took.
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
{% if settings %}SETTINGS {{ settings|format_settings }}{% endif %}"""
)


@pytest.mark.parametrize("with_append", [True, False])
@pytest.mark.parametrize("create_target_table", [True, False])
@pytest.mark.parametrize("if_not_exists", [True, False])
@pytest.mark.parametrize("on_cluster", [True, False])
@pytest.mark.parametrize(
    "depends_on", [None, ["dummy_rmv"], ["default.dummy_rmv", "src1"]]
)
@pytest.mark.parametrize("randomize", [True, False])
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
    randomize,
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
        refresh_interval="EVERY 1 HOUR" + " 30 MINUTE" if randomize else None,
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

    alter_sql = ALTER_RMV_TEMPLATE.render(
        table_name="test_rmv",
        if_not_exists=if_not_exists,
        db_name=database_name,
        refresh_interval="EVERY 1 HOUR" + " 30 MINUTE" if randomize else None,
        depends_on=depends_on,
        select_query="SELECT * FROM src1",
        on_cluster="test_cluster" if on_cluster else None,
        # settings={'setting1':'value1', 'setting2': 'value2'},
    )

    node1_1.query(alter_sql)
    show_create_after = node1_1.query("SHOW CREATE test_rmv")

    compare_create_all_nodes()
    assert show_create == show_create_after
    # breakpoint()

    pass


def compare_dates(
    date1: str | datetime,
    date2: str | datetime,
    inaccuracy=timedelta(minutes=10),
    format_str="%Y-%m-%d %H:%M:%S",
) -> bool:
    """
    Compares two dates with an inaccuracy of 2 minutes.
    """
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
    return node.query_with_retry(
        f"SELECT * FROM system.view_refreshes WHERE view='{table}'",
        check_callback=lambda r: r.iloc[0]["status"] == "Scheduled",
        parse=True,
    ).to_dict("records")[0]


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
        assert rmv["last_success_duration_ms"] > 0
        inserted_data = node1_1.query(f"SELECT * FROM {tgt}", parse=True)
        print(inserted_data)
        assert len(inserted_data) == 2

    if empty:
        assert rmv["last_refresh_time"] is None
        assert rmv["last_success_duration_ms"] is None

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
    # node1_1.query("SYSTEM TEST VIEW test_rmv set fake time '2050-01-01 00:00:01';")
    #     system test view a set fake time '2050-01-01 00:00:01';

    now = datetime.utcnow()

    inserted_data = node1_1.query(f"SELECT * FROM {tgt}", parse=True)
    if with_append:
        assert len(inserted_data) == 4
    else:
        assert len(inserted_data) == 2

    # alter time

    breakpoint()


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


INTERVALS = [
    # Same units
    # "EVERY 1 YEAR 2 YEAR",
    # "AFTER 1 YEAR 2 YEAR"
    "EVERY 1 MINUTE",
    "EVERY 1 HOUR",
    "EVERY 1 DAY",
    "EVERY 1 MONTH",
    "EVERY 1 YEAR",
    "EVERY 1 MONTH OFFSET 5 DAY 2 HOUR 30 MINUTE 15 SECONDS",
    # smth wrong with
    # "EVERY 1 WEEK",
    # "EVERY 2 WEEK",
    # "EVERY 3 WEEK",
    # "EVERY 4 WEEK",
    # "EVERY 5 WEEK",
    # "EVERY 6 WEEK",
    # "EVERY 7 WEEK",
    # "EVERY 8 WEEK",
    # "EVERY 9 WEEK",
    # "EVERY 10 WEEK",
    # "EVERY 11 WEEK",
    # "EVERY 12 WEEK",
    # "EVERY 13 WEEK",
    # "EVERY 14 WEEK",
    # "EVERY 15 WEEK",
    # "EVERY 20 WEEK",
    # "EVERY 21 WEEK",
    # "EVERY 31 WEEK",
    # "EVERY 32 WEEK",
    # "EVERY 33 WEEK",
    # "EVERY 50 WEEK",
    # "EVERY 51 WEEK",
    # "EVERY 52 WEEK",
    # "EVERY 59 WEEK",
    #
    "EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE",
    "EVERY 1 MONTH OFFSET 1 WEEK",
    "EVERY 1 MONTH OFFSET 2 WEEK",
    #
    "AFTER 30 SECONDS",
    "AFTER 30 MINUTE",
    "AFTER 2 HOUR",
    "AFTER 2 DAY",
    "AFTER 2 WEEK",
    "AFTER 2 MONTH",
    "AFTER 2 YEAR",
    "AFTER 2 MONTH",
    "AFTER 2 MONTH 3 DAY",
    "AFTER 2 MONTH 3 DAY RANDOMIZE FOR 1 DAY",
    "AFTER 1 YEAR RANDOMIZE FOR 11 MONTH",
    # Randomize bigger than interval
    "AFTER 1 MINUTE RANDOMIZE FOR 3 YEAR",
    "EVERY 1 MINUTE RANDOMIZE FOR 3 YEAR",
    "EVERY 1 MINUTE OFFSET 1 SECONDS RANDOMIZE FOR 3 YEAR",
    #
    "EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 1 HOUR",
    "EVERY 1 YEAR OFFSET 11 MONTH RANDOMIZE FOR 1 YEAR",
    "EVERY 1 YEAR OFFSET 11 MONTH RANDOMIZE FOR 1 YEAR 10 MONTH 1 DAY 1 HOUR 2 MINUTE 2 SECONDS",
    #
    "AFTER 1 YEAR 10 MONTH 3 DAY 7 HOUR 5 MINUTE 30 SECONDS RANDOMIZE FOR 2 YEAR 10 MONTH 1 DAY 1 HOUR 2 MINUTE 2 SECONDS",
    # "EVERY 1 YEAR 10 MONTH 3 DAY 7 HOUR 5 MINUTE 30 SECONDS OFFSET 11 MONTH RANDOMIZE FOR 2 YEAR 10 MONTH 1 DAY 1 HOUR 2 MINUTE 2 SECONDS",
    #
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
    #
    "AFTER 1 MONTH",
    "AFTER 1 MONTH 0 DAY",
    "AFTER 1 MONTH 1 DAY",
    "AFTER 1 MONTH 3 DAY",
    "AFTER 1 MONTH 50 DAY",
    "AFTER 1 YEAR 10 MONTH",
    "AFTER 1 YEAR 10 MONTH 3 DAY",
    "AFTER 1 YEAR 10 MONTH 3 DAY 7 HOUR",
    "AFTER 1 YEAR 10 MONTH 3 DAY 7 HOUR 5 MINUTE 30 SECONDS",
    # Interval shouldn't contain both calendar units and clock units (e.g. months and days)
    # "EVERY 1 YEAR 10 MONTH 3 DAY 7 HOUR 5 MINUTE 30 SECONDS",
]


@pytest.mark.parametrize(
    "interval",
    INTERVALS,
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

    if "WEEK" in interval:
        pytest.skip()

    def teardown():
        node1_1.query("DROP TABLE IF EXISTS test_rmv_schedule")
        # node1_1.query("DROP TABLE IF EXISTS tgt_new ON CLUSTER test_cluster")
        # node1_1.query("TRUNCATE TABLE tgt1 ON CLUSTER test_cluster")

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

    append_expect_rows = 0

    def check_data():
        inserted_data = node1_1.query_with_retry(
            f"SELECT * FROM test_rmv_schedule",
            parse=True,
            check_callback=lambda x: len(x) > 0,
            retry_count=200,
        )

        # Check data if not EMPTY
        if append:
            # Append adds rows
            global append_expect_rows
            append_expect_rows += 2
            assert len(inserted_data) == append_expect_rows
        if not append:
            # Rewrite without append
            assert len(inserted_data) == 2

    inserted_data = node1_1.query(f"SELECT * FROM test_rmv_schedule", parse=True)
    if empty:
        assert len(inserted_data) == 0
    else:
        assert rmv["last_success_duration_ms"] > 0
        check_data()

    # Trigger next refresh
    node1_1.query(
        f"SYSTEM TEST VIEW test_rmv_schedule SET FAKE TIME '{next_refresh_time}'"
    )

    if "RANDOMIZE" not in interval:
        check_data()

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


# def test_create_replicated_table(started_cluster):
#     main_node.query(
#         "CREATE DATABASE create_replicated_table ENGINE = Replicated('/test/create_replicated_table', 'shard1', 'replica' || '1');"
#     )
#     dummy_node.query(
#         "CREATE DATABASE create_replicated_table ENGINE = Replicated('/test/create_replicated_table', 'shard1', 'replica2');"
#     )
#     assert (
#         "Explicit zookeeper_path and replica_name are specified"
#         in main_node.query_and_get_error(
#             "CREATE TABLE create_replicated_table.replicated_table (d Date, k UInt64, i32 Int32) "
#             "ENGINE=ReplicatedMergeTree('/test/tmp', 'r') ORDER BY k PARTITION BY toYYYYMM(d);"
#         )
#     )
#
#     assert (
#         "Explicit zookeeper_path and replica_name are specified"
#         in main_node.query_and_get_error(
#             "CREATE TABLE create_replicated_table.replicated_table (d Date, k UInt64, i32 Int32) "
#             "ENGINE=ReplicatedMergeTree('/test/tmp', 'r') ORDER BY k PARTITION BY toYYYYMM(d);"
#         )
#     )
#
#     assert (
#         "This syntax for *MergeTree engine is deprecated"
#         in main_node.query_and_get_error(
#             "CREATE TABLE create_replicated_table.replicated_table (d Date, k UInt64, i32 Int32) "
#             "ENGINE=ReplicatedMergeTree('/test/tmp/{shard}', '{replica}', d, k, 8192);"
#         )
#     )
#
#     main_node.query(
#         "CREATE TABLE create_replicated_table.replicated_table (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree ORDER BY k PARTITION BY toYYYYMM(d);"
#     )
#
#     expected = (
#         "CREATE TABLE create_replicated_table.replicated_table\\n(\\n    `d` Date,\\n    `k` UInt64,\\n    `i32` Int32\\n)\\n"
#         "ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\n"
#         "PARTITION BY toYYYYMM(d)\\nORDER BY k\\nSETTINGS index_granularity = 8192"
#     )
#     assert_create_query(
#         [main_node, dummy_node], "create_replicated_table.replicated_table", expected
#     )
#     # assert without replacing uuid
#     assert main_node.query(
#         "show create create_replicated_table.replicated_table"
#     ) == dummy_node.query("show create create_replicated_table.replicated_table")
#     main_node.query("DROP DATABASE create_replicated_table SYNC")
#     dummy_node.query("DROP DATABASE create_replicated_table SYNC")
#
#
# @pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
# def test_simple_alter_table(started_cluster, engine):
#     database = f"test_simple_alter_table_{engine}"
#     main_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica1');"
#     )
#     dummy_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica2');"
#     )
#     # test_simple_alter_table
#     name = f"{database}.alter_test"
#     main_node.query(
#         "CREATE TABLE {} "
#         "(CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) "
#         "ENGINE = {} PARTITION BY StartDate ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);".format(
#             name, engine
#         )
#     )
#     main_node.query("ALTER TABLE {} ADD COLUMN Added0 UInt32;".format(name))
#     main_node.query("ALTER TABLE {} ADD COLUMN Added2 UInt32;".format(name))
#     main_node.query(
#         "ALTER TABLE {} ADD COLUMN Added1 UInt32 AFTER Added0;".format(name)
#     )
#     main_node.query(
#         "ALTER TABLE {} ADD COLUMN AddedNested1 Nested(A UInt32, B UInt64) AFTER Added2;".format(
#             name
#         )
#     )
#     main_node.query(
#         "ALTER TABLE {} ADD COLUMN AddedNested1.C Array(String) AFTER AddedNested1.B;".format(
#             name
#         )
#     )
#     main_node.query(
#         "ALTER TABLE {} ADD COLUMN AddedNested2 Nested(A UInt32, B UInt64) AFTER AddedNested1;".format(
#             name
#         )
#     )
#
#     full_engine = (
#         engine
#         if not "Replicated" in engine
#         else engine + "(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')"
#     )
#     expected = (
#         "CREATE TABLE {}\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
#         "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n"
#         "    `ToDrop` UInt32,\\n    `Added0` UInt32,\\n    `Added1` UInt32,\\n    `Added2` UInt32,\\n"
#         "    `AddedNested1.A` Array(UInt32),\\n    `AddedNested1.B` Array(UInt64),\\n    `AddedNested1.C` Array(String),\\n"
#         "    `AddedNested2.A` Array(UInt32),\\n    `AddedNested2.B` Array(UInt64)\\n)\\n"
#         "ENGINE = {}\\nPARTITION BY StartDate\\nORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)\\n"
#         "SETTINGS index_granularity = 8192".format(name, full_engine)
#     )
#
#     assert_create_query([main_node, dummy_node], name, expected)
#
#     # test_create_replica_after_delay
#     competing_node.query(
#         f"CREATE DATABASE IF NOT EXISTS {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica3');"
#     )
#
#     main_node.query("ALTER TABLE {} ADD COLUMN Added3 UInt32;".format(name))
#     main_node.query("ALTER TABLE {} DROP COLUMN AddedNested1;".format(name))
#     main_node.query("ALTER TABLE {} RENAME COLUMN Added1 TO AddedNested1;".format(name))
#
#     full_engine = (
#         engine
#         if not "Replicated" in engine
#         else engine + "(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')"
#     )
#     expected = (
#         "CREATE TABLE {}\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
#         "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n"
#         "    `ToDrop` UInt32,\\n    `Added0` UInt32,\\n    `AddedNested1` UInt32,\\n    `Added2` UInt32,\\n"
#         "    `AddedNested2.A` Array(UInt32),\\n    `AddedNested2.B` Array(UInt64),\\n    `Added3` UInt32\\n)\\n"
#         "ENGINE = {}\\nPARTITION BY StartDate\\nORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)\\n"
#         "SETTINGS index_granularity = 8192".format(name, full_engine)
#     )
#
#     assert_create_query([main_node, dummy_node, competing_node], name, expected)
#     main_node.query(f"DROP DATABASE {database} SYNC")
#     dummy_node.query(f"DROP DATABASE {database} SYNC")
#     competing_node.query(f"DROP DATABASE {database} SYNC")
#
#
# @pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
# def test_delete_from_table(started_cluster, engine):
#     database = f"delete_from_table_{engine}"
#
#     main_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica1');"
#     )
#     dummy_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard2', 'replica1');"
#     )
#
#     name = f"{database}.delete_test"
#     main_node.query(
#         "CREATE TABLE {} "
#         "(id UInt64, value String) "
#         "ENGINE = {} PARTITION BY id%2 ORDER BY (id);".format(name, engine)
#     )
#     main_node.query("INSERT INTO TABLE {} VALUES(1, 'aaaa');".format(name))
#     main_node.query("INSERT INTO TABLE {} VALUES(2, 'aaaa');".format(name))
#     dummy_node.query("INSERT INTO TABLE {} VALUES(1, 'bbbb');".format(name))
#     dummy_node.query("INSERT INTO TABLE {} VALUES(2, 'bbbb');".format(name))
#
#     main_node.query("DELETE FROM {} WHERE id=2;".format(name))
#
#     expected = "1\taaaa\n1\tbbbb"
#
#     table_for_select = name
#     if not "Replicated" in engine:
#         table_for_select = f"cluster('{database}', {name})"
#     for node in [main_node, dummy_node]:
#         assert_eq_with_retry(
#             node,
#             "SELECT * FROM {} ORDER BY id, value;".format(table_for_select),
#             expected,
#         )
#
#     main_node.query(f"DROP DATABASE {database} SYNC")
#     dummy_node.query(f"DROP DATABASE {database} SYNC")
#
#
# def get_table_uuid(database, name):
#     return main_node.query(
#         f"SELECT uuid FROM system.tables WHERE database = '{database}' and name = '{name}'"
#     ).strip()
#
#
# @pytest.fixture(scope="module", name="attachable_part")
# def fixture_attachable_part(started_cluster):
#     main_node.query(f"CREATE DATABASE testdb_attach_atomic ENGINE = Atomic")
#     main_node.query(
#         f"CREATE TABLE testdb_attach_atomic.test (CounterID UInt32) ENGINE = MergeTree ORDER BY (CounterID)"
#     )
#     main_node.query(f"INSERT INTO testdb_attach_atomic.test VALUES (123)")
#     main_node.query(
#         f"ALTER TABLE testdb_attach_atomic.test FREEZE WITH NAME 'test_attach'"
#     )
#     table_uuid = get_table_uuid("testdb_attach_atomic", "test")
#     return os.path.join(
#         main_node.path,
#         f"database/shadow/test_attach/store/{table_uuid[:3]}/{table_uuid}/all_1_1_0",
#     )
#
#
# @pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
# def test_alter_attach(started_cluster, attachable_part, engine):
#     database = f"alter_attach_{engine}"
#     main_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica1');"
#     )
#     dummy_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica2');"
#     )
#
#     main_node.query(
#         f"CREATE TABLE {database}.alter_attach_test (CounterID UInt32) ENGINE = {engine} ORDER BY (CounterID)"
#     )
#     table_uuid = get_table_uuid(database, "alter_attach_test")
#     # Provide and attach a part to the main node
#     shutil.copytree(
#         attachable_part,
#         os.path.join(
#             main_node.path,
#             f"database/store/{table_uuid[:3]}/{table_uuid}/detached/all_1_1_0",
#         ),
#     )
#     main_node.query(f"ALTER TABLE {database}.alter_attach_test ATTACH PART 'all_1_1_0'")
#     # On the main node, data is attached
#     assert (
#         main_node.query(f"SELECT CounterID FROM {database}.alter_attach_test")
#         == "123\n"
#     )
#     # On the other node, data is replicated only if using a Replicated table engine
#     if engine == "ReplicatedMergeTree":
#         assert (
#             dummy_node.query(f"SELECT CounterID FROM {database}.alter_attach_test")
#             == "123\n"
#         )
#     else:
#         assert (
#             dummy_node.query(f"SELECT CounterID FROM {database}.alter_attach_test")
#             == ""
#         )
#     main_node.query(f"DROP DATABASE {database} SYNC")
#     dummy_node.query(f"DROP DATABASE {database} SYNC")
#
#
# @pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
# def test_alter_drop_part(started_cluster, engine):
#     database = f"alter_drop_part_{engine}"
#     main_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica1');"
#     )
#     dummy_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica2');"
#     )
#
#     part_name = "all_0_0_0" if engine == "ReplicatedMergeTree" else "all_1_1_0"
#     main_node.query(
#         f"CREATE TABLE {database}.alter_drop_part (CounterID UInt32) ENGINE = {engine} ORDER BY (CounterID)"
#     )
#     main_node.query(f"INSERT INTO {database}.alter_drop_part VALUES (123)")
#     if engine == "MergeTree":
#         dummy_node.query(f"INSERT INTO {database}.alter_drop_part VALUES (456)")
#     else:
#         main_node.query(f"SYSTEM SYNC REPLICA {database}.alter_drop_part PULL")
#     main_node.query(f"ALTER TABLE {database}.alter_drop_part DROP PART '{part_name}'")
#     assert main_node.query(f"SELECT CounterID FROM {database}.alter_drop_part") == ""
#     if engine == "ReplicatedMergeTree":
#         # The DROP operation is still replicated at the table engine level
#         assert (
#             dummy_node.query(f"SELECT CounterID FROM {database}.alter_drop_part") == ""
#         )
#     else:
#         assert (
#             dummy_node.query(f"SELECT CounterID FROM {database}.alter_drop_part")
#             == "456\n"
#         )
#     main_node.query(f"DROP DATABASE {database} SYNC")
#     dummy_node.query(f"DROP DATABASE {database} SYNC")
#
#
# @pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
# def test_alter_detach_part(started_cluster, engine):
#     database = f"alter_detach_part_{engine}"
#     main_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica1');"
#     )
#     dummy_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica2');"
#     )
#
#     part_name = "all_0_0_0" if engine == "ReplicatedMergeTree" else "all_1_1_0"
#     main_node.query(
#         f"CREATE TABLE {database}.alter_detach (CounterID UInt32) ENGINE = {engine} ORDER BY (CounterID)"
#     )
#     main_node.query(f"INSERT INTO {database}.alter_detach VALUES (123)")
#     if engine == "MergeTree":
#         dummy_node.query(f"INSERT INTO {database}.alter_detach VALUES (456)")
#     main_node.query(f"ALTER TABLE {database}.alter_detach DETACH PART '{part_name}'")
#     detached_parts_query = f"SELECT name FROM system.detached_parts WHERE database='{database}' AND table='alter_detach'"
#     assert main_node.query(detached_parts_query) == f"{part_name}\n"
#     if engine == "ReplicatedMergeTree":
#         # The detach operation is still replicated at the table engine level
#         assert dummy_node.query(detached_parts_query) == f"{part_name}\n"
#     else:
#         assert dummy_node.query(detached_parts_query) == ""
#     main_node.query(f"DROP DATABASE {database} SYNC")
#     dummy_node.query(f"DROP DATABASE {database} SYNC")
#
#
# @pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
# def test_alter_drop_detached_part(started_cluster, engine):
#     database = f"alter_drop_detached_part_{engine}"
#     main_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica1');"
#     )
#     dummy_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica2');"
#     )
#
#     part_name = "all_0_0_0" if engine == "ReplicatedMergeTree" else "all_1_1_0"
#     main_node.query(
#         f"CREATE TABLE {database}.alter_drop_detached (CounterID UInt32) ENGINE = {engine} ORDER BY (CounterID)"
#     )
#     main_node.query(f"INSERT INTO {database}.alter_drop_detached VALUES (123)")
#     main_node.query(
#         f"ALTER TABLE {database}.alter_drop_detached DETACH PART '{part_name}'"
#     )
#     if engine == "MergeTree":
#         dummy_node.query(f"INSERT INTO {database}.alter_drop_detached VALUES (456)")
#         dummy_node.query(
#             f"ALTER TABLE {database}.alter_drop_detached DETACH PART '{part_name}'"
#         )
#     main_node.query(
#         f"ALTER TABLE {database}.alter_drop_detached DROP DETACHED PART '{part_name}'"
#     )
#     detached_parts_query = f"SELECT name FROM system.detached_parts WHERE database='{database}' AND table='alter_drop_detached'"
#     assert main_node.query(detached_parts_query) == ""
#     assert dummy_node.query(detached_parts_query) == f"{part_name}\n"
#
#     main_node.query(f"DROP DATABASE {database} SYNC")
#     dummy_node.query(f"DROP DATABASE {database} SYNC")
#
#
# @pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
# def test_alter_drop_partition(started_cluster, engine):
#     database = f"alter_drop_partition_{engine}"
#     main_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica1');"
#     )
#     dummy_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica2');"
#     )
#     snapshotting_node.query(
#         f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard2', 'replica1');"
#     )
#
#     main_node.query(
#         f"CREATE TABLE {database}.alter_drop (CounterID UInt32) ENGINE = {engine} ORDER BY (CounterID)"
#     )
#     main_node.query(f"INSERT INTO {database}.alter_drop VALUES (123)")
#     if engine == "MergeTree":
#         dummy_node.query(f"INSERT INTO {database}.alter_drop VALUES (456)")
#     snapshotting_node.query(f"INSERT INTO {database}.alter_drop VALUES (789)")
#     main_node.query(
#         f"ALTER TABLE {database}.alter_drop ON CLUSTER {database} DROP PARTITION ID 'all'",
#         settings={"replication_alter_partitions_sync": 2},
#     )
#     assert (
#         main_node.query(
#             f"SELECT CounterID FROM clusterAllReplicas('{database}', {database}.alter_drop)"
#         )
#         == ""
#     )
#     assert dummy_node.query(f"SELECT CounterID FROM {database}.alter_drop") == ""
#     main_node.query(f"DROP DATABASE {database}")
#     dummy_node.query(f"DROP DATABASE {database}")
#     snapshotting_node.query(f"DROP DATABASE {database}")
#
#
# def test_alter_fetch(started_cluster):
#     main_node.query(
#         "CREATE DATABASE alter_fetch ENGINE = Replicated('/test/alter_fetch', 'shard1', 'replica1');"
#     )
#     dummy_node.query(
#         "CREATE DATABASE alter_fetch ENGINE = Replicated('/test/alter_fetch', 'shard1', 'replica2');"
#     )
#
#     main_node.query(
#         "CREATE TABLE alter_fetch.fetch_source (CounterID UInt32) ENGINE = ReplicatedMergeTree ORDER BY (CounterID)"
#     )
#     main_node.query(
#         "CREATE TABLE alter_fetch.fetch_target (CounterID UInt32) ENGINE = ReplicatedMergeTree ORDER BY (CounterID)"
#     )
#     main_node.query("INSERT INTO alter_fetch.fetch_source VALUES (123)")
#     table_uuid = get_table_uuid("alter_fetch", "fetch_source")
#     main_node.query(
#         f"ALTER TABLE alter_fetch.fetch_target FETCH PART 'all_0_0_0' FROM '/clickhouse/tables/{table_uuid}/{{shard}}' "
#     )
#     detached_parts_query = "SELECT name FROM system.detached_parts WHERE database='alter_fetch' AND table='fetch_target'"
#     assert main_node.query(detached_parts_query) == "all_0_0_0\n"
#     assert dummy_node.query(detached_parts_query) == ""
#
#     main_node.query("DROP DATABASE alter_fetch SYNC")
#     dummy_node.query("DROP DATABASE alter_fetch SYNC")
#
#
# def test_alters_from_different_replicas(started_cluster):
#     main_node.query(
#         "CREATE DATABASE alters_from_different_replicas ENGINE = Replicated('/test/alters_from_different_replicas', 'shard1', 'replica1');"
#     )
#     dummy_node.query(
#         "CREATE DATABASE alters_from_different_replicas ENGINE = Replicated('/test/alters_from_different_replicas', 'shard1', 'replica2');"
#     )
#
#     # test_alters_from_different_replicas
#     competing_node.query(
#         "CREATE DATABASE alters_from_different_replicas ENGINE = Replicated('/test/alters_from_different_replicas', 'shard1', 'replica3');"
#     )
#
#     main_node.query(
#         "CREATE TABLE alters_from_different_replicas.concurrent_test "
#         "(CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) "
#         "ENGINE = MergeTree PARTITION BY toYYYYMM(StartDate) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);"
#     )
#
#     main_node.query(
#         "CREATE TABLE alters_from_different_replicas.dist AS alters_from_different_replicas.concurrent_test ENGINE = Distributed(alters_from_different_replicas, alters_from_different_replicas, concurrent_test, CounterID)"
#     )
#
#     dummy_node.stop_clickhouse(kill=True)
#
#     settings = {"distributed_ddl_task_timeout": 5}
#     assert "is not finished on 1 of 3 hosts" in competing_node.query_and_get_error(
#         "ALTER TABLE alters_from_different_replicas.concurrent_test ADD COLUMN Added0 UInt32;",
#         settings=settings,
#     )
#     settings = {
#         "distributed_ddl_task_timeout": 5,
#         "distributed_ddl_output_mode": "null_status_on_timeout",
#     }
#     assert "shard1\treplica2\tQUEUED\t" in main_node.query(
#         "ALTER TABLE alters_from_different_replicas.concurrent_test ADD COLUMN Added2 UInt32;",
#         settings=settings,
#     )
#     settings = {
#         "distributed_ddl_task_timeout": 5,
#         "distributed_ddl_output_mode": "never_throw",
#     }
#     assert "shard1\treplica2\tQUEUED\t" in competing_node.query(
#         "ALTER TABLE alters_from_different_replicas.concurrent_test ADD COLUMN Added1 UInt32 AFTER Added0;",
#         settings=settings,
#     )
#     dummy_node.start_clickhouse()
#     main_node.query(
#         "ALTER TABLE alters_from_different_replicas.concurrent_test ADD COLUMN AddedNested1 Nested(A UInt32, B UInt64) AFTER Added2;"
#     )
#     competing_node.query(
#         "ALTER TABLE alters_from_different_replicas.concurrent_test ADD COLUMN AddedNested1.C Array(String) AFTER AddedNested1.B;"
#     )
#     main_node.query(
#         "ALTER TABLE alters_from_different_replicas.concurrent_test ADD COLUMN AddedNested2 Nested(A UInt32, B UInt64) AFTER AddedNested1;"
#     )
#
#     expected = (
#         "CREATE TABLE alters_from_different_replicas.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
#         "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32,\\n"
#         "    `Added0` UInt32,\\n    `Added1` UInt32,\\n    `Added2` UInt32,\\n    `AddedNested1.A` Array(UInt32),\\n"
#         "    `AddedNested1.B` Array(UInt64),\\n    `AddedNested1.C` Array(String),\\n    `AddedNested2.A` Array(UInt32),\\n"
#         "    `AddedNested2.B` Array(UInt64)\\n)\\n"
#         "ENGINE = MergeTree\\nPARTITION BY toYYYYMM(StartDate)\\nORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)\\nSETTINGS index_granularity = 8192"
#     )
#
#     assert_create_query(
#         [main_node, competing_node],
#         "alters_from_different_replicas.concurrent_test",
#         expected,
#     )
#
#     # test_create_replica_after_delay
#     main_node.query("DROP TABLE alters_from_different_replicas.concurrent_test SYNC")
#     main_node.query(
#         "CREATE TABLE alters_from_different_replicas.concurrent_test "
#         "(CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) "
#         "ENGINE = ReplicatedMergeTree ORDER BY CounterID;"
#     )
#
#     expected = (
#         "CREATE TABLE alters_from_different_replicas.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
#         "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32\\n)\\n"
#         "ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\nORDER BY CounterID\\nSETTINGS index_granularity = 8192"
#     )
#
#     assert_create_query(
#         [main_node, competing_node],
#         "alters_from_different_replicas.concurrent_test",
#         expected,
#     )
#
#     main_node.query(
#         "INSERT INTO alters_from_different_replicas.dist (CounterID, StartDate, UserID) SELECT number, addDays(toDate('2020-02-02'), number), intHash32(number) FROM numbers(10)"
#     )
#
#     # test_replica_restart
#     main_node.restart_clickhouse()
#
#     expected = (
#         "CREATE TABLE alters_from_different_replicas.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
#         "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32\\n)\\n"
#         "ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\nORDER BY CounterID\\nSETTINGS index_granularity = 8192"
#     )
#
#     # test_snapshot_and_snapshot_recover
#     snapshotting_node.query(
#         "CREATE DATABASE alters_from_different_replicas ENGINE = Replicated('/test/alters_from_different_replicas', 'shard2', 'replica1');"
#     )
#     snapshot_recovering_node.query(
#         "CREATE DATABASE alters_from_different_replicas ENGINE = Replicated('/test/alters_from_different_replicas', 'shard2', 'replica2');"
#     )
#     assert_create_query(
#         all_nodes, "alters_from_different_replicas.concurrent_test", expected
#     )
#
#     main_node.query("SYSTEM FLUSH DISTRIBUTED alters_from_different_replicas.dist")
#     main_node.query(
#         "ALTER TABLE alters_from_different_replicas.concurrent_test UPDATE StartDate = addYears(StartDate, 1) WHERE 1"
#     )
#     res = main_node.query(
#         "ALTER TABLE alters_from_different_replicas.concurrent_test DELETE WHERE UserID % 2"
#     )
#     assert (
#         "shard1\treplica1\tOK" in res
#         and "shard1\treplica2\tOK" in res
#         and "shard1\treplica3\tOK" in res
#     )
#     assert "shard2\treplica1\tOK" in res and "shard2\treplica2\tOK" in res
#
#     expected = (
#         "1\t1\tmain_node\n"
#         "1\t2\tdummy_node\n"
#         "1\t3\tcompeting_node\n"
#         "2\t1\tsnapshotting_node\n"
#         "2\t2\tsnapshot_recovering_node\n"
#     )
#     assert (
#         main_node.query(
#             "SELECT shard_num, replica_num, host_name FROM system.clusters WHERE cluster='alters_from_different_replicas'"
#         )
#         == expected
#     )
#
#     # test_drop_and_create_replica
#     main_node.query("DROP DATABASE alters_from_different_replicas SYNC")
#     main_node.query(
#         "CREATE DATABASE alters_from_different_replicas ENGINE = Replicated('/test/alters_from_different_replicas', 'shard1', 'replica1');"
#     )
#
#     expected = (
#         "CREATE TABLE alters_from_different_replicas.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
#         "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32\\n)\\n"
#         "ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\nORDER BY CounterID\\nSETTINGS index_granularity = 8192"
#     )
#
#     assert_create_query(
#         [main_node, competing_node],
#         "alters_from_different_replicas.concurrent_test",
#         expected,
#     )
#     assert_create_query(
#         all_nodes, "alters_from_different_replicas.concurrent_test", expected
#     )
#
#     for node in all_nodes:
#         node.query("SYSTEM SYNC REPLICA alters_from_different_replicas.concurrent_test")
#
#     expected = (
#         "0\t2021-02-02\t4249604106\n"
#         "1\t2021-02-03\t1343103100\n"
#         "4\t2021-02-06\t3902320246\n"
#         "7\t2021-02-09\t3844986530\n"
#         "9\t2021-02-11\t1241149650\n"
#     )
#
#     assert_eq_with_retry(
#         dummy_node,
#         "SELECT CounterID, StartDate, UserID FROM alters_from_different_replicas.dist ORDER BY CounterID",
#         expected,
#     )
#     main_node.query("DROP DATABASE alters_from_different_replicas SYNC")
#     dummy_node.query("DROP DATABASE alters_from_different_replicas SYNC")
#     competing_node.query("DROP DATABASE alters_from_different_replicas SYNC")
#     snapshotting_node.query("DROP DATABASE alters_from_different_replicas SYNC")
#     snapshot_recovering_node.query("DROP DATABASE alters_from_different_replicas SYNC")
#
#
# def create_some_tables(db):
#     settings = {
#         "distributed_ddl_task_timeout": 0,
#         "allow_experimental_object_type": 1,
#         "allow_suspicious_codecs": 1,
#     }
#     main_node.query(f"CREATE TABLE {db}.t1 (n int) ENGINE=Memory", settings=settings)
#     dummy_node.query(
#         f"CREATE TABLE {db}.t2 (s String) ENGINE=Memory", settings=settings
#     )
#     main_node.query(
#         f"CREATE TABLE {db}.mt1 (n int) ENGINE=MergeTree order by n",
#         settings=settings,
#     )
#     dummy_node.query(
#         f"CREATE TABLE {db}.mt2 (n int) ENGINE=MergeTree order by n",
#         settings=settings,
#     )
#     main_node.query(
#         f"CREATE TABLE {db}.rmt1 (n int) ENGINE=ReplicatedMergeTree order by n",
#         settings=settings,
#     )
#     dummy_node.query(
#         f"CREATE TABLE {db}.rmt2 (n int CODEC(ZSTD, ZSTD, ZSTD(12), LZ4HC(12))) ENGINE=ReplicatedMergeTree order by n",
#         settings=settings,
#     )
#     main_node.query(
#         f"CREATE TABLE {db}.rmt3 (n int, json Object('json') materialized '') ENGINE=ReplicatedMergeTree order by n",
#         settings=settings,
#     )
#     dummy_node.query(
#         f"CREATE TABLE {db}.rmt5 (n int) ENGINE=ReplicatedMergeTree order by n",
#         settings=settings,
#     )
#     main_node.query(
#         f"CREATE MATERIALIZED VIEW {db}.mv1 (n int) ENGINE=ReplicatedMergeTree order by n AS SELECT n FROM recover.rmt1",
#         settings=settings,
#     )
#     dummy_node.query(
#         f"CREATE MATERIALIZED VIEW {db}.mv2 (n int) ENGINE=ReplicatedMergeTree order by n  AS SELECT n FROM recover.rmt2",
#         settings=settings,
#     )
#     main_node.query(
#         f"CREATE DICTIONARY {db}.d1 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
#         "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'rmt1' PASSWORD '' DB 'recover')) "
#         "LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT())"
#     )
#     dummy_node.query(
#         f"CREATE DICTIONARY {db}.d2 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
#         "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'rmt2' PASSWORD '' DB 'recover')) "
#         "LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT())"
#     )
#
#
# # These tables are used to check that DatabaseReplicated correctly renames all the tables in case when it restores from the lost state
# def create_table_for_exchanges(db):
#     settings = {"distributed_ddl_task_timeout": 0}
#     for table in ["a1", "a2", "a3", "a4", "a5", "a6"]:
#         main_node.query(
#             f"CREATE TABLE {db}.{table} (s String) ENGINE=ReplicatedMergeTree order by s",
#             settings=settings,
#         )
#
#
# def test_recover_staled_replica(started_cluster):
#     main_node.query(
#         "CREATE DATABASE recover ENGINE = Replicated('/clickhouse/databases/recover', 'shard1', 'replica1');"
#     )
#     started_cluster.get_kazoo_client("zoo1").set(
#         "/clickhouse/databases/recover/logs_to_keep", b"10"
#     )
#     dummy_node.query(
#         "CREATE DATABASE recover ENGINE = Replicated('/clickhouse/databases/recover', 'shard1', 'replica2');"
#     )
#
#     settings = {"distributed_ddl_task_timeout": 0}
#     create_some_tables("recover")
#     create_table_for_exchanges("recover")
#
#     for table in ["t1", "t2", "mt1", "mt2", "rmt1", "rmt2", "rmt3", "rmt5"]:
#         main_node.query(f"INSERT INTO recover.{table} VALUES (42)")
#     for table in ["t1", "t2", "mt1", "mt2"]:
#         dummy_node.query(f"INSERT INTO recover.{table} VALUES (42)")
#
#     for i, table in enumerate(["a1", "a2", "a3", "a4", "a5", "a6"]):
#         main_node.query(f"INSERT INTO recover.{table} VALUES ('{str(i + 1) * 10}')")
#
#     for table in ["rmt1", "rmt2", "rmt3", "rmt5"]:
#         main_node.query(f"SYSTEM SYNC REPLICA recover.{table}")
#     for table in ["a1", "a2", "a3", "a4", "a5", "a6"]:
#         main_node.query(f"SYSTEM SYNC REPLICA recover.{table}")
#
#     with PartitionManager() as pm:
#         pm.drop_instance_zk_connections(dummy_node)
#         dummy_node.query_and_get_error("RENAME TABLE recover.t1 TO recover.m1")
#
#         main_node.query_with_retry(
#             "RENAME TABLE recover.t1 TO recover.m1", settings=settings
#         )
#         main_node.query_with_retry(
#             "ALTER TABLE recover.mt1  ADD COLUMN m int", settings=settings
#         )
#         main_node.query_with_retry(
#             "ALTER TABLE recover.rmt1 ADD COLUMN m int", settings=settings
#         )
#         main_node.query_with_retry(
#             "RENAME TABLE recover.rmt3 TO recover.rmt4", settings=settings
#         )
#         main_node.query_with_retry("DROP TABLE recover.rmt5", settings=settings)
#         main_node.query_with_retry("DROP DICTIONARY recover.d2", settings=settings)
#         main_node.query_with_retry(
#             "CREATE DICTIONARY recover.d2 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
#             "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'rmt1' PASSWORD '' DB 'recover')) "
#             "LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT());",
#             settings=settings,
#         )
#
#         inner_table = (
#             ".inner_id."
#             + dummy_node.query_with_retry(
#                 "SELECT uuid FROM system.tables WHERE database='recover' AND name='mv1'"
#             ).strip()
#         )
#         main_node.query_with_retry(
#             f"ALTER TABLE recover.`{inner_table}` MODIFY COLUMN n int DEFAULT 42",
#             settings=settings,
#         )
#         main_node.query_with_retry(
#             "ALTER TABLE recover.mv1 MODIFY QUERY SELECT m as n FROM recover.rmt1",
#             settings=settings,
#         )
#         main_node.query_with_retry(
#             "RENAME TABLE recover.mv2 TO recover.mv3",
#             settings=settings,
#         )
#
#         main_node.query_with_retry(
#             "CREATE TABLE recover.tmp AS recover.m1", settings=settings
#         )
#         main_node.query_with_retry("DROP TABLE recover.tmp", settings=settings)
#         main_node.query_with_retry(
#             "CREATE TABLE recover.tmp AS recover.m1", settings=settings
#         )
#         main_node.query_with_retry("DROP TABLE recover.tmp", settings=settings)
#         main_node.query_with_retry(
#             "CREATE TABLE recover.tmp AS recover.m1", settings=settings
#         )
#
#         main_node.query("EXCHANGE TABLES recover.a1 AND recover.a2", settings=settings)
#         main_node.query("EXCHANGE TABLES recover.a3 AND recover.a4", settings=settings)
#         main_node.query("EXCHANGE TABLES recover.a5 AND recover.a4", settings=settings)
#         main_node.query("EXCHANGE TABLES recover.a6 AND recover.a3", settings=settings)
#         main_node.query("RENAME TABLE recover.a6 TO recover.a7", settings=settings)
#         main_node.query("RENAME TABLE recover.a1 TO recover.a8", settings=settings)
#
#     assert (
#         main_node.query(
#             "SELECT name FROM system.tables WHERE database='recover' AND name NOT LIKE '.inner_id.%' ORDER BY name"
#         )
#         == "a2\na3\na4\na5\na7\na8\nd1\nd2\nm1\nmt1\nmt2\nmv1\nmv3\nrmt1\nrmt2\nrmt4\nt2\ntmp\n"
#     )
#     query = (
#         "SELECT name, uuid, create_table_query FROM system.tables WHERE database='recover' AND name NOT LIKE '.inner_id.%' "
#         "ORDER BY name SETTINGS show_table_uuid_in_table_create_query_if_not_nil=1"
#     )
#     expected = main_node.query(query)
#     assert_eq_with_retry(dummy_node, query, expected)
#     assert (
#         main_node.query(
#             "SELECT count() FROM system.tables WHERE database='recover' AND name LIKE '.inner_id.%'"
#         )
#         == "2\n"
#     )
#     assert (
#         dummy_node.query(
#             "SELECT count() FROM system.tables WHERE database='recover' AND name LIKE '.inner_id.%'"
#         )
#         == "2\n"
#     )
#
#     # Check that Database Replicated renamed all the tables correctly
#     for i, table in enumerate(["a2", "a8", "a5", "a7", "a4", "a3"]):
#         assert (
#             dummy_node.query(f"SELECT * FROM recover.{table}") == f"{str(i + 1) * 10}\n"
#         )
#
#     for table in [
#         "m1",
#         "t2",
#         "mt1",
#         "mt2",
#         "rmt1",
#         "rmt2",
#         "rmt4",
#         "d1",
#         "d2",
#         "mv1",
#         "mv3",
#     ]:
#         assert main_node.query(f"SELECT (*,).1 FROM recover.{table}") == "42\n"
#     for table in ["t2", "rmt1", "rmt2", "rmt4", "d1", "d2", "mt2", "mv1", "mv3"]:
#         assert (
#             dummy_node.query(f"SELECT '{table}', (*,).1 FROM recover.{table}")
#             == f"{table}\t42\n"
#         )
#     for table in ["m1", "mt1"]:
#         assert dummy_node.query(f"SELECT count() FROM recover.{table}") == "0\n"
#     global test_recover_staled_replica_run
#     assert (
#         dummy_node.query(
#             "SELECT count() FROM system.tables WHERE database='recover_broken_tables'"
#         )
#         == f"{test_recover_staled_replica_run}\n"
#     )
#     assert (
#         dummy_node.query(
#             "SELECT count() FROM system.tables WHERE database='recover_broken_replicated_tables'"
#         )
#         == f"{test_recover_staled_replica_run}\n"
#     )
#     test_recover_staled_replica_run += 1
#
#     print(dummy_node.query("SHOW DATABASES"))
#     print(dummy_node.query("SHOW TABLES FROM recover_broken_tables"))
#     print(dummy_node.query("SHOW TABLES FROM recover_broken_replicated_tables"))
#
#     table = dummy_node.query(
#         "SHOW TABLES FROM recover_broken_tables LIKE 'mt1_41_%' LIMIT 1"
#     ).strip()
#     assert (
#         dummy_node.query(f"SELECT (*,).1 FROM recover_broken_tables.{table}") == "42\n"
#     )
#     table = dummy_node.query(
#         "SHOW TABLES FROM recover_broken_replicated_tables LIKE 'rmt5_41_%' LIMIT 1"
#     ).strip()
#     assert (
#         dummy_node.query(f"SELECT (*,).1 FROM recover_broken_replicated_tables.{table}")
#         == "42\n"
#     )
#
#     expected = "Cleaned 6 outdated objects: dropped 1 dictionaries and 3 tables, moved 2 tables"
#     assert_logs_contain(dummy_node, expected)
#
#     dummy_node.query("DROP TABLE recover.tmp")
#     assert_eq_with_retry(
#         main_node,
#         "SELECT count() FROM system.tables WHERE database='recover' AND name='tmp'",
#         "0\n",
#     )
#     main_node.query("DROP DATABASE recover SYNC")
#     dummy_node.query("DROP DATABASE recover SYNC")
#
#
# def test_recover_staled_replica_many_mvs(started_cluster):
#     main_node.query("DROP DATABASE IF EXISTS recover_mvs")
#     dummy_node.query("DROP DATABASE IF EXISTS recover_mvs")
#
#     main_node.query_with_retry(
#         "CREATE DATABASE IF NOT EXISTS recover_mvs ENGINE = Replicated('/clickhouse/databases/recover_mvs', 'shard1', 'replica1');"
#     )
#     started_cluster.get_kazoo_client("zoo1").set(
#         "/clickhouse/databases/recover_mvs/logs_to_keep", b"10"
#     )
#     dummy_node.query_with_retry(
#         "CREATE DATABASE IF NOT EXISTS recover_mvs ENGINE = Replicated('/clickhouse/databases/recover_mvs', 'shard1', 'replica2');"
#     )
#
#     settings = {"distributed_ddl_task_timeout": 0}
#
#     with PartitionManager() as pm:
#         pm.drop_instance_zk_connections(dummy_node)
#         dummy_node.query_and_get_error("RENAME TABLE recover_mvs.t1 TO recover_mvs.m1")
#
#         for identifier in ["1", "2", "3", "4"]:
#             main_node.query(
#                 f"CREATE TABLE recover_mvs.rmt{identifier} (n int) ENGINE=ReplicatedMergeTree ORDER BY n",
#                 settings=settings,
#             )
#
#         print("Created tables")
#
#         for identifier in ["1", "2", "3", "4"]:
#             main_node.query(
#                 f"CREATE TABLE recover_mvs.mv_inner{identifier} (n int) ENGINE=ReplicatedMergeTree ORDER BY n",
#                 settings=settings,
#             )
#
#         for identifier in ["1", "2", "3", "4"]:
#             main_node.query_with_retry(
#                 f"""CREATE MATERIALIZED VIEW recover_mvs.mv{identifier}
#                     TO recover_mvs.mv_inner{identifier}
#                     AS SELECT * FROM recover_mvs.rmt{identifier}""",
#                 settings=settings,
#             )
#
#         print("Created MVs")
#
#         for identifier in ["1", "2", "3", "4"]:
#             main_node.query_with_retry(
#                 f"""CREATE VIEW recover_mvs.view_from_mv{identifier}
#                     AS SELECT * FROM recover_mvs.mv{identifier}""",
#                 settings=settings,
#             )
#
#         print("Created Views on top of MVs")
#
#         for identifier in ["1", "2", "3", "4"]:
#             main_node.query_with_retry(
#                 f"""CREATE MATERIALIZED VIEW recover_mvs.cascade_mv{identifier}
#                     ENGINE=MergeTree() ORDER BY tuple()
#                     POPULATE AS SELECT * FROM recover_mvs.mv_inner{identifier};""",
#                 settings=settings,
#             )
#
#         print("Created cascade MVs")
#
#         for identifier in ["1", "2", "3", "4"]:
#             main_node.query_with_retry(
#                 f"""CREATE VIEW recover_mvs.view_from_cascade_mv{identifier}
#                     AS SELECT * FROM recover_mvs.cascade_mv{identifier}""",
#                 settings=settings,
#             )
#
#         print("Created Views on top of cascade MVs")
#
#         for identifier in ["1", "2", "3", "4"]:
#             main_node.query_with_retry(
#                 f"""CREATE MATERIALIZED VIEW recover_mvs.double_cascade_mv{identifier}
#                     ENGINE=MergeTree() ORDER BY tuple()
#                     POPULATE AS SELECT * FROM recover_mvs.`.inner_id.{get_table_uuid("recover_mvs", f"cascade_mv{identifier}")}`""",
#                 settings=settings,
#             )
#
#         print("Created double cascade MVs")
#
#         for identifier in ["1", "2", "3", "4"]:
#             main_node.query_with_retry(
#                 f"""CREATE VIEW recover_mvs.view_from_double_cascade_mv{identifier}
#                     AS SELECT * FROM recover_mvs.double_cascade_mv{identifier}""",
#                 settings=settings,
#             )
#
#         print("Created Views on top of double cascade MVs")
#
#         # This weird table name is actually makes sence because it starts with letter `a` and may break some internal sorting
#         main_node.query_with_retry(
#             """
#             CREATE VIEW recover_mvs.anime
#             AS
#             SELECT n
#             FROM
#             (
#                 SELECT *
#                 FROM
#                 (
#                     SELECT *
#                     FROM
#                     (
#                         SELECT *
#                         FROM recover_mvs.mv_inner1 AS q1
#                         INNER JOIN recover_mvs.mv_inner2 AS q2 ON q1.n = q2.n
#                     ) AS new_table_1
#                     INNER JOIN recover_mvs.mv_inner3 AS q3 ON new_table_1.n = q3.n
#                 ) AS new_table_2
#                 INNER JOIN recover_mvs.mv_inner4 AS q4 ON new_table_2.n = q4.n
#             )
#             """,
#             settings=settings,
#         )
#
#         print("Created final boss")
#
#         for identifier in ["1", "2", "3", "4"]:
#             main_node.query_with_retry(
#                 f"""CREATE DICTIONARY recover_mvs.`11111d{identifier}` (n UInt64)
#                 PRIMARY KEY n
#                 SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'double_cascade_mv{identifier}' DB 'recover_mvs'))
#                 LAYOUT(FLAT()) LIFETIME(1)""",
#                 settings=settings,
#             )
#
#         print("Created dictionaries")
#
#         for identifier in ["1", "2", "3", "4"]:
#             main_node.query_with_retry(
#                 f"""CREATE VIEW recover_mvs.`00000vd{identifier}`
#                 AS SELECT * FROM recover_mvs.`11111d{identifier}`""",
#                 settings=settings,
#             )
#
#         print("Created Views on top of dictionaries")
#
#     dummy_node.query("SYSTEM SYNC DATABASE REPLICA recover_mvs")
#     query = "SELECT name FROM system.tables WHERE database='recover_mvs' ORDER BY name"
#     assert main_node.query(query) == dummy_node.query(query)
#
#     main_node.query("DROP DATABASE IF EXISTS recover_mvs")
#     dummy_node.query("DROP DATABASE IF EXISTS recover_mvs")
#
#
# def test_startup_without_zk(started_cluster):
#     with PartitionManager() as pm:
#         pm.drop_instance_zk_connections(main_node)
#         err = main_node.query_and_get_error(
#             "CREATE DATABASE startup ENGINE = Replicated('/clickhouse/databases/startup', 'shard1', 'replica1');"
#         )
#         assert "ZooKeeper" in err or "Coordination::Exception" in err
#     main_node.query(
#         "CREATE DATABASE startup ENGINE = Replicated('/clickhouse/databases/startup', 'shard1', 'replica1');"
#     )
#     main_node.query(
#         "CREATE TABLE startup.rmt (n int) ENGINE=ReplicatedMergeTree order by n"
#     )
#
#     main_node.query("INSERT INTO startup.rmt VALUES (42)")
#     with PartitionManager() as pm:
#         pm.drop_instance_zk_connections(main_node)
#         main_node.restart_clickhouse(stop_start_wait_sec=60)
#         assert main_node.query("SELECT (*,).1 FROM startup.rmt") == "42\n"
#
#     # we need to wait until the table is not readonly
#     main_node.query_with_retry("INSERT INTO startup.rmt VALUES(42)")
#
#     main_node.query_with_retry("CREATE TABLE startup.m (n int) ENGINE=Memory")
#
#     main_node.query("EXCHANGE TABLES startup.rmt AND startup.m")
#     assert main_node.query("SELECT (*,).1 FROM startup.m") == "42\n"
#
#     main_node.query("DROP DATABASE startup SYNC")
#
#
# def test_server_uuid(started_cluster):
#     uuid1 = main_node.query("select serverUUID()")
#     uuid2 = dummy_node.query("select serverUUID()")
#     assert uuid1 != uuid2
#     main_node.restart_clickhouse()
#     uuid1_after_restart = main_node.query("select serverUUID()")
#     assert uuid1 == uuid1_after_restart
#
#
# def test_sync_replica(started_cluster):
#     main_node.query(
#         "CREATE DATABASE test_sync_database ENGINE = Replicated('/test/sync_replica', 'shard1', 'replica1');"
#     )
#     dummy_node.query(
#         "CREATE DATABASE test_sync_database ENGINE = Replicated('/test/sync_replica', 'shard1', 'replica2');"
#     )
#
#     number_of_tables = 1000
#
#     settings = {"distributed_ddl_task_timeout": 0}
#
#     with PartitionManager() as pm:
#         pm.drop_instance_zk_connections(dummy_node)
#
#         for i in range(number_of_tables):
#             main_node.query(
#                 "CREATE TABLE test_sync_database.table_{} (n int) ENGINE=MergeTree order by n".format(
#                     i
#                 ),
#                 settings=settings,
#             )
#
#     # wait for host to reconnect
#     dummy_node.query_with_retry("SELECT * FROM system.zookeeper WHERE path='/'")
#
#     dummy_node.query("SYSTEM SYNC DATABASE REPLICA test_sync_database")
#
#     assert "2\n" == main_node.query(
#         "SELECT sum(is_active) FROM system.clusters WHERE cluster='test_sync_database'"
#     )
#
#     assert dummy_node.query(
#         "SELECT count() FROM system.tables where database='test_sync_database'"
#     ).strip() == str(number_of_tables)
#
#     assert main_node.query(
#         "SELECT count() FROM system.tables where database='test_sync_database'"
#     ).strip() == str(number_of_tables)
#
#     engine_settings = {"default_table_engine": "ReplicatedMergeTree"}
#     dummy_node.query(
#         "CREATE TABLE test_sync_database.table (n int, primary key n) partition by n",
#         settings=engine_settings,
#     )
#     main_node.query("INSERT INTO test_sync_database.table SELECT * FROM numbers(10)")
#     dummy_node.query("TRUNCATE TABLE test_sync_database.table", settings=settings)
#     dummy_node.query(
#         "ALTER TABLE test_sync_database.table ADD COLUMN m int", settings=settings
#     )
#
#     main_node.query(
#         "SYSTEM SYNC DATABASE REPLICA ON CLUSTER test_sync_database test_sync_database"
#     )
#
#     lp1 = main_node.query(
#         "select value from system.zookeeper where path='/test/sync_replica/replicas/shard1|replica1' and name='log_ptr'"
#     )
#     lp2 = main_node.query(
#         "select value from system.zookeeper where path='/test/sync_replica/replicas/shard1|replica2' and name='log_ptr'"
#     )
#     max_lp = main_node.query(
#         "select value from system.zookeeper where path='/test/sync_replica/' and name='max_log_ptr'"
#     )
#     assert lp1 == max_lp
#     assert lp2 == max_lp
#
#     main_node.query("DROP DATABASE test_sync_database SYNC")
#     dummy_node.query("DROP DATABASE test_sync_database SYNC")
#
#
# def test_force_synchronous_settings(started_cluster):
#     main_node.query(
#         "CREATE DATABASE test_force_synchronous_settings ENGINE = Replicated('/clickhouse/databases/test2', 'shard1', 'replica1');"
#     )
#     dummy_node.query(
#         "CREATE DATABASE test_force_synchronous_settings ENGINE = Replicated('/clickhouse/databases/test2', 'shard1', 'replica2');"
#     )
#     snapshotting_node.query(
#         "CREATE DATABASE test_force_synchronous_settings ENGINE = Replicated('/clickhouse/databases/test2', 'shard2', 'replica1');"
#     )
#     main_node.query(
#         "CREATE TABLE test_force_synchronous_settings.t (n int) ENGINE=ReplicatedMergeTree('/test/same/path/{shard}', '{replica}') ORDER BY tuple()"
#     )
#     main_node.query(
#         "INSERT INTO test_force_synchronous_settings.t SELECT * FROM numbers(10)"
#     )
#     snapshotting_node.query(
#         "INSERT INTO test_force_synchronous_settings.t SELECT * FROM numbers(10)"
#     )
#     snapshotting_node.query(
#         "SYSTEM SYNC DATABASE REPLICA test_force_synchronous_settings"
#     )
#     dummy_node.query("SYSTEM SYNC DATABASE REPLICA test_force_synchronous_settings")
#
#     snapshotting_node.query("SYSTEM STOP MERGES test_force_synchronous_settings.t")
#
#     def start_merges_func():
#         time.sleep(5)
#         snapshotting_node.query("SYSTEM START MERGES test_force_synchronous_settings.t")
#
#     start_merges_thread = threading.Thread(target=start_merges_func)
#     start_merges_thread.start()
#
#     settings = {
#         "mutations_sync": 2,
#         "database_replicated_enforce_synchronous_settings": 1,
#     }
#     main_node.query(
#         "ALTER TABLE test_force_synchronous_settings.t UPDATE n = n * 10 WHERE 1",
#         settings=settings,
#     )
#     assert "10\t450\n" == snapshotting_node.query(
#         "SELECT count(), sum(n) FROM test_force_synchronous_settings.t"
#     )
#     start_merges_thread.join()
#
#     def select_func():
#         dummy_node.query(
#             "SELECT sleepEachRow(1) FROM test_force_synchronous_settings.t SETTINGS function_sleep_max_microseconds_per_block = 0"
#         )
#
#     select_thread = threading.Thread(target=select_func)
#     select_thread.start()
#
#     settings = {"database_replicated_enforce_synchronous_settings": 1}
#     snapshotting_node.query(
#         "DROP TABLE test_force_synchronous_settings.t SYNC", settings=settings
#     )
#     main_node.query(
#         "CREATE TABLE test_force_synchronous_settings.t (n String) ENGINE=ReplicatedMergeTree('/test/same/path/{shard}', '{replica}') ORDER BY tuple()"
#     )
#     select_thread.join()
#
#
# def test_recover_digest_mismatch(started_cluster):
#     main_node.query("DROP DATABASE IF EXISTS recover_digest_mismatch")
#     dummy_node.query("DROP DATABASE IF EXISTS recover_digest_mismatch")
#
#     main_node.query(
#         "CREATE DATABASE recover_digest_mismatch ENGINE = Replicated('/clickhouse/databases/recover_digest_mismatch', 'shard1', 'replica1');"
#     )
#     dummy_node.query(
#         "CREATE DATABASE recover_digest_mismatch ENGINE = Replicated('/clickhouse/databases/recover_digest_mismatch', 'shard1', 'replica2');"
#     )
#
#     create_some_tables("recover_digest_mismatch")
#
#     main_node.query("SYSTEM SYNC DATABASE REPLICA recover_digest_mismatch")
#     dummy_node.query("SYSTEM SYNC DATABASE REPLICA recover_digest_mismatch")
#
#     ways_to_corrupt_metadata = [
#         "mv /var/lib/clickhouse/metadata/recover_digest_mismatch/t1.sql /var/lib/clickhouse/metadata/recover_digest_mismatch/m1.sql",
#         "sed --follow-symlinks -i 's/Int32/String/' /var/lib/clickhouse/metadata/recover_digest_mismatch/mv1.sql",
#         "rm -f /var/lib/clickhouse/metadata/recover_digest_mismatch/d1.sql",
#         "rm -rf /var/lib/clickhouse/metadata/recover_digest_mismatch/",  # Will trigger "Directory already exists"
#         "rm -rf /var/lib/clickhouse/store",
#     ]
#
#     for command in ways_to_corrupt_metadata:
#         print(f"Corrupting data using `{command}`")
#         need_remove_is_active_node = "rm -rf" in command
#         dummy_node.stop_clickhouse(kill=not need_remove_is_active_node)
#         dummy_node.exec_in_container(["bash", "-c", command])
#
#         query = (
#             "SELECT name, uuid, create_table_query FROM system.tables WHERE database='recover_digest_mismatch' AND name NOT LIKE '.inner_id.%' "
#             "ORDER BY name SETTINGS show_table_uuid_in_table_create_query_if_not_nil=1"
#         )
#         expected = main_node.query(query)
#
#         if need_remove_is_active_node:
#             # NOTE Otherwise it fails to recreate ReplicatedMergeTree table due to "Replica already exists"
#             main_node.query(
#                 "SYSTEM DROP REPLICA '2' FROM DATABASE recover_digest_mismatch"
#             )
#
#         # There is a race condition between deleting active node and creating it on server startup
#         # So we start a server only after we deleted all table replicas from the Keeper
#         dummy_node.start_clickhouse()
#         assert_eq_with_retry(dummy_node, query, expected)
#
#     main_node.query("DROP DATABASE IF EXISTS recover_digest_mismatch")
#     dummy_node.query("DROP DATABASE IF EXISTS recover_digest_mismatch")
#
#     print("Everything Okay")
#
#
# def test_replicated_table_structure_alter(started_cluster):
#     main_node.query("DROP DATABASE IF EXISTS table_structure")
#     dummy_node.query("DROP DATABASE IF EXISTS table_structure")
#
#     main_node.query(
#         "CREATE DATABASE table_structure ENGINE = Replicated('/clickhouse/databases/table_structure', 'shard1', 'replica1');"
#     )
#     dummy_node.query(
#         "CREATE DATABASE table_structure ENGINE = Replicated('/clickhouse/databases/table_structure', 'shard1', 'replica2');"
#     )
#     competing_node.query(
#         "CREATE DATABASE table_structure ENGINE = Replicated('/clickhouse/databases/table_structure', 'shard1', 'replica3');"
#     )
#
#     competing_node.query("CREATE TABLE table_structure.mem (n int) ENGINE=Memory")
#     dummy_node.query("DETACH DATABASE table_structure")
#
#     settings = {"distributed_ddl_task_timeout": 0}
#     main_node.query(
#         "CREATE TABLE table_structure.rmt (n int, v UInt64) ENGINE=ReplicatedReplacingMergeTree(v) ORDER BY n",
#         settings=settings,
#     )
#
#     competing_node.query("SYSTEM SYNC DATABASE REPLICA table_structure")
#     competing_node.query("DETACH DATABASE table_structure")
#
#     main_node.query(
#         "ALTER TABLE table_structure.rmt ADD COLUMN m int", settings=settings
#     )
#     main_node.query(
#         "ALTER TABLE table_structure.rmt COMMENT COLUMN v 'version'", settings=settings
#     )
#     main_node.query("INSERT INTO table_structure.rmt VALUES (1, 2, 3)")
#
#     command = "rm -f /var/lib/clickhouse/metadata/table_structure/mem.sql"
#     competing_node.exec_in_container(["bash", "-c", command])
#     competing_node.restart_clickhouse(kill=True)
#
#     dummy_node.query("ATTACH DATABASE table_structure")
#     dummy_node.query("SYSTEM SYNC DATABASE REPLICA table_structure")
#     dummy_node.query("SYSTEM SYNC REPLICA table_structure.rmt")
#     assert "1\t2\t3\n" == dummy_node.query("SELECT * FROM table_structure.rmt")
#
#     competing_node.query("SYSTEM SYNC DATABASE REPLICA table_structure")
#     competing_node.query("SYSTEM SYNC REPLICA table_structure.rmt")
#     # time.sleep(600)
#     assert "mem" in competing_node.query("SHOW TABLES FROM table_structure")
#     assert "1\t2\t3\n" == competing_node.query("SELECT * FROM table_structure.rmt")
#
#     main_node.query("ALTER TABLE table_structure.rmt ADD COLUMN k int")
#     main_node.query("INSERT INTO table_structure.rmt VALUES (1, 2, 3, 4)")
#     dummy_node.query("SYSTEM SYNC DATABASE REPLICA table_structure")
#     dummy_node.query("SYSTEM SYNC REPLICA table_structure.rmt")
#     assert "1\t2\t3\t0\n1\t2\t3\t4\n" == dummy_node.query(
#         "SELECT * FROM table_structure.rmt ORDER BY k"
#     )
#
#
# def test_modify_comment(started_cluster):
#     main_node.query(
#         "CREATE DATABASE modify_comment_db ENGINE = Replicated('/test/modify_comment', 'shard1', 'replica' || '1');"
#     )
#
#     dummy_node.query(
#         "CREATE DATABASE modify_comment_db ENGINE = Replicated('/test/modify_comment', 'shard1', 'replica' || '2');"
#     )
#
#     main_node.query(
#         "CREATE TABLE modify_comment_db.modify_comment_table (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree ORDER BY k PARTITION BY toYYYYMM(d);"
#     )
#
#     def restart_verify_not_readonly():
#         main_node.restart_clickhouse()
#         assert (
#             main_node.query(
#                 "SELECT is_readonly FROM system.replicas WHERE table = 'modify_comment_table'"
#             )
#             == "0\n"
#         )
#         dummy_node.restart_clickhouse()
#         assert (
#             dummy_node.query(
#                 "SELECT is_readonly FROM system.replicas WHERE table = 'modify_comment_table'"
#             )
#             == "0\n"
#         )
#
#     main_node.query(
#         "ALTER TABLE modify_comment_db.modify_comment_table COMMENT COLUMN d 'Some comment'"
#     )
#
#     restart_verify_not_readonly()
#
#     main_node.query(
#         "ALTER TABLE modify_comment_db.modify_comment_table MODIFY COMMENT 'Some error comment'"
#     )
#
#     restart_verify_not_readonly()
#
#     main_node.query("DROP DATABASE modify_comment_db SYNC")
#     dummy_node.query("DROP DATABASE modify_comment_db SYNC")
#
#
# def test_table_metadata_corruption(started_cluster):
#     main_node.query("DROP DATABASE IF EXISTS table_metadata_corruption")
#     dummy_node.query("DROP DATABASE IF EXISTS table_metadata_corruption")
#
#     main_node.query(
#         "CREATE DATABASE table_metadata_corruption ENGINE = Replicated('/clickhouse/databases/table_metadata_corruption', 'shard1', 'replica1');"
#     )
#     dummy_node.query(
#         "CREATE DATABASE table_metadata_corruption ENGINE = Replicated('/clickhouse/databases/table_metadata_corruption', 'shard1', 'replica2');"
#     )
#
#     create_some_tables("table_metadata_corruption")
#
#     main_node.query("SYSTEM SYNC DATABASE REPLICA table_metadata_corruption")
#     dummy_node.query("SYSTEM SYNC DATABASE REPLICA table_metadata_corruption")
#
#     # Server should handle this by throwing an exception during table loading, which should lead to server shutdown
#     corrupt = "sed --follow-symlinks -i 's/ReplicatedMergeTree/CorruptedMergeTree/' /var/lib/clickhouse/metadata/table_metadata_corruption/rmt1.sql"
#
#     print(f"Corrupting metadata using `{corrupt}`")
#     dummy_node.stop_clickhouse(kill=True)
#     dummy_node.exec_in_container(["bash", "-c", corrupt])
#
#     query = (
#         "SELECT name, uuid, create_table_query FROM system.tables WHERE database='table_metadata_corruption' AND name NOT LIKE '.inner_id.%' "
#         "ORDER BY name SETTINGS show_table_uuid_in_table_create_query_if_not_nil=1"
#     )
#     expected = main_node.query(query)
#
#     # We expect clickhouse server to shutdown without LOGICAL_ERRORs or deadlocks
#     dummy_node.start_clickhouse(expected_to_fail=True)
#     assert not dummy_node.contains_in_log("LOGICAL_ERROR")
#
#     fix_corrupt = "sed --follow-symlinks -i 's/CorruptedMergeTree/ReplicatedMergeTree/' /var/lib/clickhouse/metadata/table_metadata_corruption/rmt1.sql"
#     print(f"Fix corrupted metadata using `{fix_corrupt}`")
#     dummy_node.exec_in_container(["bash", "-c", fix_corrupt])
#
#     dummy_node.start_clickhouse()
#     assert_eq_with_retry(dummy_node, query, expected)
#
#     main_node.query("DROP DATABASE IF EXISTS table_metadata_corruption")
#     dummy_node.query("DROP DATABASE IF EXISTS table_metadata_corruption")
