#!/usr/bin/env bash
# A filtered SELECT must not use more memory just because the table has many columns whose names look
# like map key subcolumns. Peak memory is compared between two tables that differ only in whether
# their wide column names have that shape, so it must not grow with the number of skip indexes.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

INDEXES=128
WIDE_COLUMNS=250
PADDING=$(printf 'p%.0s' $(seq 1 135))

# Wide columns named `attrs.val_...` in one table and `attrs.key_...` in the other. Only the second
# spelling looks like a key of a map named `attrs`, and neither table has a map at all.
build_columns() {
    local kind=$1
    for i in $(seq 1 $WIDE_COLUMNS); do
        printf ', `attrs.%s_%s_%s` UInt64' "$kind" "$PADDING" "$i"
    done
}

build_indexes() {
    for i in $(seq 1 $INDEXES); do
        printf ', INDEX i%s s%s TYPE bloom_filter GRANULARITY 1' "$i" "$(( i % 10 + 1 ))"
    done
}

NARROW=$(for i in $(seq 1 10); do printf ', s%s String' "$i"; done)
INDEX_LIST=$(build_indexes)
PREDICATE=$(for i in $(seq 1 10); do [ "$i" -gt 1 ] && printf ' AND '; printf "s%s = 'x'" "$i"; done)

for kind in val key; do
    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE t_$kind (id UInt64 $NARROW $(build_columns $kind) $INDEX_LIST)
        ENGINE = MergeTree ORDER BY id"
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_$kind SELECT * FROM generateRandom() LIMIT 1"
done

for kind in val key; do
    # First run per table warms shared caches; the second is the one measured.
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM t_$kind WHERE $PREDICATE" > /dev/null
    $CLICKHOUSE_CLIENT --query_id "${CLICKHOUSE_DATABASE}_$kind" -q "
        SELECT count() FROM t_$kind WHERE $PREDICATE" > /dev/null
done

# The memory ratio only separates while all $INDEXES conditions are retained at once, and nothing above
# asserts that. Pin it: an index appears in `EXPLAIN indexes = 1` only when it survived
# `alwaysUnknownOrTrue` and reached `skip_indexes.useful_indices`.
$CLICKHOUSE_CLIENT -q "
    SELECT
        (SELECT countIf(trim(explain) ILIKE 'Name: i%')
         FROM (EXPLAIN indexes = 1 SELECT count() FROM t_val WHERE $PREDICATE)) = $INDEXES,
        (SELECT countIf(trim(explain) ILIKE 'Name: i%')
         FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key WHERE $PREDICATE)) = $INDEXES
    SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT -q "
    SELECT countIf(query_id = '${CLICKHOUSE_DATABASE}_val') >= 1
       AND countIf(query_id = '${CLICKHOUSE_DATABASE}_key') >= 1
       AND maxIf(memory_usage, query_id = '${CLICKHOUSE_DATABASE}_key')
         < maxIf(memory_usage, query_id = '${CLICKHOUSE_DATABASE}_val') * 2
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND query_id IN ('${CLICKHOUSE_DATABASE}_val', '${CLICKHOUSE_DATABASE}_key')
      AND type = 'QueryFinish'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_val"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_key"
