#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/105368
# Hive partitioning failed to parse ISO 8601 timestamps with timezone suffixes
# (e.g. +0000, +00:00, Z) because it used strict `Basic` parsing regardless of
# `cast_string_to_date_time_mode`. Hive partition value extraction is
# conceptually a string-to-type cast, so it should honour that setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=${CLICKHOUSE_TMP}/$CLICKHOUSE_TEST_UNIQUE_NAME
mkdir -p "$DATA_DIR/ts=2026-05-07T22:15:15+0000"
mkdir -p "$DATA_DIR/ts=2026-05-07T22:15:15+00:00"
mkdir -p "$DATA_DIR/ts=2026-05-07T22:15:15Z"
mkdir -p "$DATA_DIR/ts=2026-05-07T22:15:15"

echo -e "id\n1" > "$DATA_DIR/ts=2026-05-07T22:15:15+0000/data.csv"
echo -e "id\n2" > "$DATA_DIR/ts=2026-05-07T22:15:15+00:00/data.csv"
echo -e "id\n3" > "$DATA_DIR/ts=2026-05-07T22:15:15Z/data.csv"
echo -e "id\n4" > "$DATA_DIR/ts=2026-05-07T22:15:15/data.csv"

# Inferred type case: schema does not declare `ts`, so it is inferred from the path.
$CLICKHOUSE_LOCAL -q "
SELECT id, ts, toTypeName(ts)
FROM file('$DATA_DIR/ts=*/data.csv')
ORDER BY id
SETTINGS use_hive_partitioning = 1;
"

# Schema-declared type case: `ts` is declared as DateTime64.
$CLICKHOUSE_LOCAL -q "
SELECT id, ts, toTypeName(ts)
FROM file('$DATA_DIR/ts=*/data.csv', 'CSV', 'id UInt64, ts DateTime64(0, ''UTC'')')
ORDER BY id
SETTINGS use_hive_partitioning = 1;
"

# Same as above but with cast_string_to_date_time_mode = 'best_effort' set explicitly.
$CLICKHOUSE_LOCAL -q "
SELECT id, ts
FROM file('$DATA_DIR/ts=*/data.csv', 'CSV', 'id UInt64, ts DateTime64(0, ''UTC'')')
ORDER BY id
SETTINGS use_hive_partitioning = 1, cast_string_to_date_time_mode = 'best_effort';
"

# If the user explicitly opts into the strict parser, parsing of timezone
# suffixes must fail (so this query is expected to error).
$CLICKHOUSE_LOCAL -q "
SELECT id, ts FROM file(
    '$DATA_DIR/ts=2026-05-07T22:15:15+0000/data.csv',
    'CSV',
    'id UInt64, ts DateTime64(0, ''UTC'')')
SETTINGS use_hive_partitioning = 1, cast_string_to_date_time_mode = 'basic';
" 2>&1 | grep -c "TYPE_MISMATCH"

rm -rf "$DATA_DIR"
