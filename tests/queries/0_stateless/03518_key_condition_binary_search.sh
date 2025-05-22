#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t;
CREATE TABLE t
(
    c Enum8('Zero' = 0, 'One' = 1, 'Two' = 2, 'Three' = 3, 'Four' = 4, 'Five' = 5)
)
ENGINE = MergeTree
ORDER BY c;
INSERT INTO t values('One');
SELECT * FROM t WHERE c = 1 settings send_logs_level='trace';
SELECT * FROM t WHERE c = 'One' settings send_logs_level='trace';
SELECT * FROM t WHERE c = 1 and 1 = 1 settings send_logs_level='trace';
" 2>&1 | grep -c "binary search"

$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t1;
CREATE TABLE t1
(
    timestamp DateTime64(3, 'Asia/Shanghai')
)
ENGINE = MergeTree
ORDER BY timestamp;
INSERT INTO t1 VALUES ('2025-05-21 00:00:00');
SELECT * FROM t1 WHERE toDayOfMonth(timestamp) = 1 settings send_logs_level='trace';
" 2>&1 | grep -c "generic exclusion search"