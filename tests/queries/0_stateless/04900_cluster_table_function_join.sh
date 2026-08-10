#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$DIR"

# Asymmetric fixtures with per-side marker columns: a symmetric one hides column misalignment.
printf '1,L1\n2,L2\n3,L3\n' > "$DIR/left.csv"
printf '1,R1\n2,R2\n3,R3\n4,R4\n5,R5\n' > "$DIR/right.csv"

C="test_cluster_two_shards_localhost"
L="fileCluster('$C', '${CLICKHOUSE_TEST_UNIQUE_NAME}/left.csv', 'CSV', 'a UInt32, lm String')"
R="fileCluster('$C', '${CLICKHOUSE_TEST_UNIQUE_NAME}/right.csv', 'CSV', 'a UInt32, rm String')"
PL="file('${CLICKHOUSE_TEST_UNIQUE_NAME}/left.csv', 'CSV', 'a UInt32, lm String')"
PR="file('${CLICKHOUSE_TEST_UNIQUE_NAME}/right.csv', 'CSV', 'a UInt32, rm String')"

# Both analyzers must agree, so the reference is a self-checking oracle. Shapes are emitted as SQL
# into a single session: a client process per query costs far more than the queries themselves.
run() {
    printf "SELECT '-- %s';\n" "$1"
    for analyzer in 0 1; do
        printf 'SET enable_analyzer = %s;\n%s;\n' "$analyzer" "$2"
    done
}

{
run "INNER" "SELECT a, lm, rm FROM $L AS x INNER JOIN $R AS y USING (a) ORDER BY a FORMAT CSV"
run "LEFT" "SELECT a, lm, rm FROM $L AS x LEFT JOIN $R AS y USING (a) ORDER BY a FORMAT CSV"
run "RIGHT" "SELECT a, lm, rm FROM $L AS x RIGHT JOIN $R AS y USING (a) ORDER BY a FORMAT CSV"
run "FULL" "SELECT a, lm, rm FROM $L AS x FULL JOIN $R AS y USING (a) ORDER BY a FORMAT CSV"
run "CROSS" "SELECT count() FROM $L AS x CROSS JOIN $R AS y"
run "self join on the same file" "SELECT a, x.lm, y.lm FROM $L AS x INNER JOIN $L AS y USING (a) ORDER BY a FORMAT CSV"
run "three cluster sides" "SELECT count() FROM $L AS x, $R AS y, $R AS z"

# A temporary table exists only on the initiator, so this fails unless the shard query is
# stripped of the join rather than merely being planned on the initiator.
printf 'CREATE TEMPORARY TABLE tt (a UInt32, tm String) ENGINE = Memory;\n'
printf "INSERT INTO tt VALUES (1, 'T1'), (2, 'T2');\n"
run "JOIN a temporary table" "SELECT a, lm, tm FROM $L AS x INNER JOIN tt AS y USING (a) ORDER BY a FORMAT CSV"

run "aggregation over the join" "SELECT a, sum(length(lm) + length(rm)) FROM $L AS x INNER JOIN $R AS y USING (a) GROUP BY a ORDER BY a FORMAT CSV"
run "WHERE spanning both sides" "SELECT a, lm, rm FROM $L AS x INNER JOIN $R AS y ON x.a = y.a WHERE x.a > 1 AND y.a < 3 ORDER BY a FORMAT CSV"
run "HAVING" "SELECT a, count() AS c FROM $L AS x INNER JOIN $R AS y USING (a) GROUP BY a HAVING c = 1 ORDER BY a FORMAT CSV"
run "ORDER BY and LIMIT" "SELECT a, lm, rm FROM $L AS x INNER JOIN $R AS y USING (a) ORDER BY a DESC LIMIT 2 FORMAT CSV"

printf "SELECT '-- shapes that must not change';\n"
run "single cluster function" "SELECT * FROM $L ORDER BY a FORMAT CSV"
run "cluster JOIN plain file" "SELECT a, lm, rm FROM $L AS x INNER JOIN $PR AS y USING (a) ORDER BY a FORMAT CSV"
run "plain file JOIN cluster" "SELECT a, lm, rm FROM $PL AS x INNER JOIN $R AS y USING (a) ORDER BY a FORMAT CSV"
run "ARRAY JOIN over a cluster function" "SELECT a, e FROM (SELECT a, [1, 2] AS arr FROM $L) ARRAY JOIN arr AS e ORDER BY a, e FORMAT CSV"

# Without a join, aggregation is still pushed to the shards.
run "aggregation is still pushed down" "SELECT trimLeft(explain) FROM (EXPLAIN PLAN SELECT sum(a) FROM $L) WHERE explain ILIKE '%MergingAggregated%' OR explain ILIKE '%ReadFromCluster%'"
} | $CLICKHOUSE_CLIENT

rm -rf "$DIR"
