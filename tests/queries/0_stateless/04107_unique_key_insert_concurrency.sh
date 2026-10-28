#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY: parallel INSERTs into one partition are serialized by its partition guard; 10 clients
# insert the same 500 keys, and each key survives once.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_unique_key 1 --optimize_trivial_count_query 0 --optimize_use_implicit_projections 0"

# Red if concurrent INSERTs into one partition stop being serialized (`overlap` rows exceed 500).
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_concurrent"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_concurrent (id UInt32, client UInt32)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             parts_to_delay_insert = 10000, parts_to_throw_insert = 20000
"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES uk_concurrent"

CLIENTS=10
KEY_MOD=500

for c in $(seq 1 $CLIENTS); do
    (
        $CLICKHOUSE_CLIENT --query "
            INSERT INTO uk_concurrent
            SELECT number AS id, ${c} AS client
            FROM numbers(${KEY_MOD})
        "
    ) &
done
wait

$CLICKHOUSE_CLIENT --query "SELECT 'overlap', count(), count(DISTINCT id) FROM uk_concurrent"   # overlap 500 500

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_concurrent"
