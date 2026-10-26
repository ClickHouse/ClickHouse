#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A right side that reads through ReadFromPreparedSource cannot be cloned, so the direct join
# must decline it and report the same refusal as any other unsupported storage. The assertion is
# on the message: both the refusal and the clone failure are NOT_IMPLEMENTED, so a bare
# serverError annotation cannot tell them apart. The refusal is matched on the invariant part of
# the wording rather than the whole sentence, which has already been reworded once.
expect_refusal() {
    local label="$1"
    local query="$2"
    local out
    out=$(${CLICKHOUSE_CLIENT} --query "$query" 2>&1)

    if [[ "$out" == *"Cannot clone"* ]]; then
        echo "$label: clone failure"
    elif [[ "$out" == *"(NOT_IMPLEMENTED)"* && "$out" == *"storage type"* ]]; then
        echo "$label: graceful refusal"
    else
        echo "$label: unexpected: ${out//$'\n'/ }"
    fi
}

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE t_left (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right_mt (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right_empty (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right_buf AS t_right_mt
    ENGINE = Buffer(currentDatabase(), t_right_mt, 1, 1000, 1000, 10000, 10000, 10000000, 10000000);
INSERT INTO t_left VALUES (1, 'A');
"

expect_refusal "left join, empty buffer" "
    SELECT * FROM t_left AS l LEFT JOIN t_right_buf AS r ON l.id = r.id
    SETTINGS enable_analyzer = 1, join_algorithm = 'direct'"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_right_buf VALUES (1, 'B')"

expect_refusal "left join, non-empty buffer" "
    SELECT * FROM t_left AS l LEFT JOIN t_right_buf AS r ON l.id = r.id
    SETTINGS enable_analyzer = 1, join_algorithm = 'direct'"

expect_refusal "inner join, non-empty buffer" "
    SELECT * FROM t_left AS l INNER JOIN t_right_buf AS r ON l.id = r.id
    SETTINGS enable_analyzer = 1, join_algorithm = 'direct'"

echo "--- hash join over the same shape"
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM t_left AS l LEFT JOIN t_right_buf AS r ON l.id = r.id
    ORDER BY l.id
    SETTINGS enable_analyzer = 1, join_algorithm = 'hash'"

echo "--- direct join, MergeTree right side"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO t_right_mt VALUES (1, 'C')"
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM t_left AS l LEFT JOIN t_right_mt AS r ON l.id = r.id
    ORDER BY l.id
    SETTINGS enable_analyzer = 1, join_algorithm = 'direct'"

echo "--- direct join, empty MergeTree right side"
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM t_left AS l LEFT JOIN t_right_empty AS r ON l.id = r.id
    ORDER BY l.id
    SETTINGS enable_analyzer = 1, join_algorithm = 'direct'"
