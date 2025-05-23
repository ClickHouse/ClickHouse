#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: the ProfileEvents with the expected values are reported on the replicas the query runs in,
# and the coordinator does not collect all ProfileEvents values.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

readonly query_prefix=$CLICKHOUSE_DATABASE

$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t;
CREATE TABLE t
(
    c Enum8('Zero' = 0, 'One' = 1, 'Two' = 2, 'Three' = 3, 'Four' = 4, 'Five' = 5)
)
ENGINE = MergeTree
ORDER BY c;
INSERT INTO t values('One');"

$CLICKHOUSE_CLIENT -n -q "SELECT * FROM t WHERE c = 1 FORMAT Null;" --query_id="${query_prefix}_binary1"
$CLICKHOUSE_CLIENT -n -q "SELECT * FROM t WHERE c = 'One' FORMAT Null;" --query_id="${query_prefix}_binary2"
$CLICKHOUSE_CLIENT -n -q "SELECT * FROM t WHERE c = 1 and 1 = 1  FORMAT Null;" --query_id="${query_prefix}_binary3"

$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t1;
CREATE TABLE t1
(
    timestamp DateTime64(3, 'Asia/Shanghai')
)
ENGINE = MergeTree
ORDER BY timestamp;
INSERT INTO t1 VALUES ('2025-05-21 00:00:00');"

$CLICKHOUSE_CLIENT -n -q "SELECT * FROM t1 WHERE toDayOfMonth(timestamp) = 1 FORMAT Null;" --query-id="${query_prefix}_generic"
$CLICKHOUSE_CLIENT -n -q "SYSTEM FLUSH LOGS query_log;"

$CLICKHOUSE_CLIENT -n -q "SELECT sum(ProfileEvents['IndexBinarySearchAlgorithm']), sum(ProfileEvents['IndexGenericExclusionSearchAlgorithm']) FROM system.query_log
    WHERE type > 1 AND event_date >= yesterday() AND query_id ILIKE '${query_prefix}_binary%' AND current_database = currentDatabase()"
$CLICKHOUSE_CLIENT -n -q "SELECT sum(ProfileEvents['IndexBinarySearchAlgorithm']), sum(ProfileEvents['IndexGenericExclusionSearchAlgorithm']) FROM system.query_log
    WHERE type > 1 AND event_date >= yesterday() AND query_id ILIKE '${query_prefix}_generic%' AND current_database = currentDatabase()"
