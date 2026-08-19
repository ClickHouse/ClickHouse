#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel-replicas
# no-replicated-database: the short ATTACH VIEW is rejected in a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
# Window Views need the old analyzer.
client="${CLICKHOUSE_CLIENT} --enable_analyzer 0 --allow_experimental_window_view 1"

$client --multiquery --query "
CREATE TABLE $db.src (k UInt64, t DateTime) ENGINE = MergeTree ORDER BY k;
CREATE WINDOW VIEW $db.wv ENGINE = MergeTree ORDER BY ts AS
    SELECT count(k) AS cnt, tumbleStart(w_id) AS ts FROM $db.src GROUP BY tumble(t, INTERVAL 5 SECOND) AS w_id;
DETACH VIEW $db.wv;
"

# The view parser cannot spell WINDOW VIEW, so the stub must not silently take the stored kind.
$client --query "ATTACH VIEW $db.wv" 2>&1 | grep -q "INCORRECT_QUERY" && echo "INCORRECT_QUERY" || echo "NO ERROR"
$client --query "SELECT count() FROM system.tables WHERE database = '$db' AND name = 'wv'"

$client --multiquery --query "
ATTACH TABLE $db.wv;
SELECT engine FROM system.tables WHERE database = '$db' AND name = 'wv';
DROP VIEW $db.wv;
DROP TABLE $db.src;
"
