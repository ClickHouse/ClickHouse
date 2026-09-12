#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SQLite has no unsigned 64-bit integer type, so the sink writes `UInt64` as text to preserve values above the
# signed 64-bit range. This test checks that a `UInt64` column of an explicit `ENGINE = SQLite` table
# round-trips through the storage engine (it reads back the exact value rather than a clamped `INT64_MAX`) and
# that numeric predicates on it are applied correctly - they must not be pushed down to SQLite, where a
# text-stored number compares lexicographically and after every numeric value.

DB_PATH="${CLICKHOUSE_TMP}/04635_sqlite_uint64.db"
rm -f "${DB_PATH}"

# SQLite stores the `UInt64` value as text (the affinity does not matter for values above the signed range).
sqlite3 "${DB_PATH}" 'CREATE TABLE t (u TEXT);'

${CLICKHOUSE_LOCAL} --query="
CREATE TABLE t (u UInt64) ENGINE = SQLite('${DB_PATH}', 't');

-- The largest value exceeds INT64_MAX; the two small values exercise range/equality filtering.
INSERT INTO t VALUES (18446744073709551615), (10), (2);

SELECT 'round-trip (exact value, not clamped):';
SELECT u FROM t ORDER BY u;

SELECT 'range filter u > 2 (must include 10 and the max value):';
SELECT u FROM t WHERE u > 2 ORDER BY u;

SELECT 'equality on the value above INT64_MAX:';
SELECT u FROM t WHERE u = 18446744073709551615;
"

rm -f "${DB_PATH}"
