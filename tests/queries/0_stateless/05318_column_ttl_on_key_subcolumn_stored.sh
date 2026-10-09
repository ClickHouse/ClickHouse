#!/usr/bin/env bash
# A column TTL on a column whose subcolumn is read by the table key is rejected when it is stated (see
# `05317_column_ttl_on_key_subcolumn`), but a table stored with such a TTL by an earlier version still has
# to load. Its TTL must not execute either: a TTL merge resets the expired values to the default, which
# changes the key of those rows and leaves the merged part unsorted. It has to fail instead, and
# `MODIFY COLUMN ... REMOVE TTL` repairs the table.
#
# `clickhouse-local` over a prepared data directory is how the stored metadata is obtained here: the
# table is created without the TTL, and its stored definition is then edited into the form an older
# server would have written.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE DATABASE db;
CREATE TABLE db.t (c0 Date, c1 Tuple(UInt8)) ENGINE = MergeTree PRIMARY KEY cityHash64(\`c1.1\`) SETTINGS index_granularity = 1;
"

sed -i 's/^    `c1` Tuple(UInt8)$/    `c1` Tuple(UInt8) TTL c0 + toIntervalSecond(8)/' "${WORKING_DIR}/metadata/db/t.sql"
grep -c 'TTL' "${WORKING_DIR}/metadata/db/t.sql"

# The table loads, and INSERT works.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "SELECT count() FROM db.t"
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
INSERT INTO db.t SELECT if(number % 2 = 0, toDate('2000-01-01'), toDate('2100-01-01')), tuple(toUInt8(number)) FROM numbers(50);
SELECT count() FROM db.t;
"

# A TTL merge fails, and no row is reset to the default.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "OPTIMIZE TABLE db.t FINAL" 2>&1 >/dev/null | grep -o -m 1 -F 'ILLEGAL_COLUMN'
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "SELECT count(), countIf(c1.1 = 0) FROM db.t"

# Dropping the TTL is the way out of it, and it works.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "ALTER TABLE db.t MODIFY COLUMN c1 REMOVE TTL"
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "OPTIMIZE TABLE db.t FINAL; SELECT count(), countIf(c1.1 = 0) FROM db.t"

rm -rf "${WORKING_DIR}"
