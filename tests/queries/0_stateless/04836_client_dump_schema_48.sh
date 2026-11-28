#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the XGBoost contrib is not built in the fast test.

# --dump-schema orders a table after the model dictionary that a SET assignment of its GROUP BY TTL reads, which CREATE
# analyzes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# A MergeTree TTL reads `enable_xgboost` from the server's own settings, so every instance gets it.
$CLICKHOUSE_LOCAL --path "$work/src" --enable_xgboost=1 -n -q "
SET enable_xgboost = 1;
CREATE DATABASE d;
CREATE TABLE d.train (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO d.train SELECT number, intDiv(number, 7), 2 * number + 3 * intDiv(number, 7) FROM numbers(100);
CREATE DICTIONARY d.zzz_model (x1 Float64, x2 Float64, y Float64) PRIMARY KEY (x1, x2)
    SOURCE(CLICKHOUSE(TABLE 'train' DB 'd')) LAYOUT(XGBOOST(num_iterations 10)) LIFETIME(0);
CREATE TABLE d.aaa_ttl (ts DateTime, k UInt64, x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY k
    TTL ts GROUP BY k SET y = max(predictXGBoost('d.zzz_model', x1, x2));"
$CLICKHOUSE_LOCAL --path "$work/src" --enable_xgboost=1 --dump-schema d > "$work/dump.sql" 2> "$work/err"
echo "dump rc=$?"
echo "order: $(grep -o -E '^CREATE (TABLE|DICTIONARY) d\.(aaa_ttl|zzz_model)' "$work/dump.sql" | sed 's/.* d\.//' | tr '\n' ' ' | sed 's/ $//')"
$CLICKHOUSE_LOCAL --path "$work/dst" --enable_xgboost=1 -n --queries-file "$work/dump.sql" > "$work/out" 2>&1
echo "replay rc=$?"
grep -o 'Code: [0-9]*' "$work/out" | head -1
