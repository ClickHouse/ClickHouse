#!/usr/bin/env bash
# Tags: atomic-database, no-fasttest, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
CREATE MATERIALIZED VIEW mv_internal_test
ENGINE = MergeTree ORDER BY x
AS SELECT number AS x FROM system.numbers LIMIT 0"

$CLICKHOUSE_CLIENT --query "RENAME TABLE mv_internal_test TO mv_internal_test_renamed"

$CLICKHOUSE_CLIENT --query "
CREATE MATERIALIZED VIEW rmv_internal_test
REFRESH AFTER 1 HOUR
ENGINE = MergeTree ORDER BY x
EMPTY
AS SELECT 1 AS x"

$CLICKHOUSE_CLIENT --query "SYSTEM REFRESH VIEW rmv_internal_test"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT VIEW rmv_internal_test"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE ts_internal_test ENGINE = TimeSeries
" --allow_experimental_time_series_table=1

$CLICKHOUSE_CLIENT --query "
CREATE TABLE wv_source (a Int32, timestamp DateTime) ENGINE = MergeTree ORDER BY tuple()"

$CLICKHOUSE_CLIENT --allow_experimental_window_view=1 --enable_analyzer=0 --query "
CREATE WINDOW VIEW wv_internal_test
INNER ENGINE MergeTree ORDER BY tuple(w_start)
AS SELECT count(a), tumbleStart(wid) AS w_start, tumbleEnd(wid) AS w_end
FROM wv_source
GROUP BY tumble(timestamp, INTERVAL '1' HOUR) AS wid"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

echo "--- Regular MV inner table CREATE ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND is_internal = 1
    AND query_kind = 'Create'
    AND query LIKE '%CREATE%TABLE%.inner_id.%'
    AND current_database = currentDatabase()
    AND type IN ('QueryStart', 'QueryFinish')"

echo "--- MV inner table RENAME ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND is_internal = 1
    AND query_kind = 'Rename'
    AND query LIKE '%RENAME TABLE%'
    AND query LIKE '%.inner_id.%'
    AND current_database = currentDatabase()
    AND type IN ('QueryStart', 'QueryFinish')"

echo "--- RMV temp table CREATE ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND is_internal = 1
    AND query_kind = 'Create'
    AND query LIKE '%CREATE%TABLE%.tmp%inner_id%'
    AND current_database = currentDatabase()
    AND type IN ('QueryStart', 'QueryFinish')"

echo "--- RMV EXCHANGE/RENAME ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND is_internal = 1
    AND query_kind = 'Rename'
    AND (query LIKE '%EXCHANGE TABLES%' OR query LIKE '%RENAME TABLE%')
    AND query LIKE '%.tmp%inner_id%'
    AND current_database = currentDatabase()
    AND type IN ('QueryStart', 'QueryFinish')"

echo "--- RMV temp table DROP ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND is_internal = 1
    AND query_kind = 'Drop'
    AND query LIKE '%DROP TABLE IF EXISTS%.tmp%inner_id%'
    AND current_database = currentDatabase()
    AND type IN ('QueryStart', 'QueryFinish')"

echo "--- TimeSeries inner table CREATE ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() >= 3
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND is_internal = 1
    AND query_kind = 'Create'
    AND (query LIKE '%inner_id.data.%' OR query LIKE '%inner_id.tags.%' OR query LIKE '%inner_id.metrics.%')
    AND current_database = currentDatabase()
    AND type IN ('QueryStart', 'QueryFinish')"

echo "--- WindowView inner table CREATE ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND is_internal = 1
    AND query_kind = 'Create'
    AND query LIKE '%CREATE%TABLE%.inner_id.%'
    AND query LIKE '%wv_internal_test%'
    AND type IN ('QueryStart', 'QueryFinish')"

echo "--- SystemLog table CREATE ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND is_internal = 1
    AND query_kind = 'Create'
    AND query LIKE '%CREATE TABLE%system.%_log%'
    AND type IN ('QueryStart', 'QueryFinish')"

$CLICKHOUSE_CLIENT --allow_experimental_window_view=1 --enable_analyzer=0 --query "DROP VIEW IF EXISTS wv_internal_test" 2>/dev/null || true
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS wv_source"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --query "DROP TABLE IF EXISTS ts_internal_test"
$CLICKHOUSE_CLIENT --query "DROP VIEW IF EXISTS rmv_internal_test"
$CLICKHOUSE_CLIENT --query "DROP VIEW IF EXISTS mv_internal_test_renamed"
