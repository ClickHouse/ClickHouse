#!/usr/bin/env bash
# Tags: stateful, long

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


ROWS=123456
SEED=$(${CLICKHOUSE_CLIENT} -q "SELECT reinterpretAsUInt32(today())")

${CLICKHOUSE_CLIENT} --max_threads 16 --query="
CREATE TABLE t1 ENGINE = MergeTree ORDER BY tuple() AS
SELECT
    sipHash64(CounterID, $SEED) AS CounterID,
    EventDate,
    sipHash64(WatchID, $SEED) AS WatchID,
    sipHash64(UserID, $SEED) AS UserID,
    URL
FROM test.hits
ORDER BY
    CounterID ASC,
    EventDate ASC
LIMIT $ROWS;

CREATE TABLE t2 ENGINE = MergeTree ORDER BY tuple() AS
SELECT
    sipHash64(CounterID, $SEED) AS CounterID,
    EventDate,
    sipHash64(WatchID, $SEED) AS WatchID,
    sipHash64(UserID, $SEED) AS UserID,
    URL
FROM test.hits
ORDER BY
    CounterID DESC,
    EventDate DESC
LIMIT $ROWS;

set max_memory_usage = 0;
set query_plan_optimize_join_order_limit = 10;

CREATE TABLE res_hash
ENGINE = MergeTree()
ORDER BY (CounterID, EventDate, WatchID, UserID, URL, t2.CounterID, t2.EventDate, t2.WatchID, t2.UserID, t2.URL)
AS SELECT
    t1.*,
    t2.*
FROM t1
LEFT JOIN t2 ON (t1.UserID = t2.UserID) AND ((t1.EventDate < t2.EventDate) OR (length(t1.URL) > length(t2.URL)))
ORDER BY ALL
LIMIT $ROWS
SETTINGS join_algorithm = 'hash', parallel_hash_join_threshold = 1000000000;

CREATE TABLE res_parallel_hash
ENGINE = MergeTree()
ORDER BY (CounterID, EventDate, WatchID, UserID, URL, t2.CounterID, t2.EventDate, t2.WatchID, t2.UserID, t2.URL)
AS SELECT
    t1.*,
    t2.*
FROM t1
LEFT JOIN t2 ON (t1.UserID = t2.UserID) AND ((t1.EventDate < t2.EventDate) OR (length(t1.URL) > length(t2.URL)))
ORDER BY ALL
LIMIT $ROWS
SETTINGS join_algorithm = 'hash', parallel_hash_join_threshold = 0;

SELECT *
FROM (
    SELECT * FROM res_hash ORDER BY ALL
    EXCEPT
    SELECT * FROM res_parallel_hash ORDER BY ALL
)
LIMIT 1;
"
