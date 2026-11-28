#!/usr/bin/env bash
# A tuple or a map as a dictionary source parameter value is refused when the definition is stated (see
# `05313_dictionary_source_parameter_collection_value`), but a dictionary whose stored definition already holds
# one still has to load at startup and attach: a short `ATTACH` evaluates a stored `tuple(...)` again, and a map
# is stored as an array.
#
# `clickhouse-local` over a prepared data directory is how the stored metadata is obtained here: the dictionary
# is created with a string, its stored definition is then edited into the form a server without this check
# wrote, and the next start loads it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE DATABASE db;
CREATE DICTIONARY db.d (id UInt64) PRIMARY KEY id
SOURCE(HTTP(URL 'http://example.test/' FORMAT 'TSV')) LAYOUT(FLAT()) LIFETIME(0);
CREATE DICTIONARY db.m (id UInt64) PRIMARY KEY id
SOURCE(HTTP(URL 'http://example.test/m' FORMAT 'TSV')) LAYOUT(FLAT()) LIFETIME(0);
"

metadata_file=$(grep -rl -F "URL 'http://example.test/'" "${WORKING_DIR}" --include='*.sql')
# Not `URL`: `create_table_query` hides a non-literal `url` whole, so the probe below could not see the tuple.
sed -i "s|FORMAT 'TSV'|FORMAT tuple('TSV')|" "${metadata_file}"
# Without this the arm would pass on an unmodified definition, i.e. assert nothing.
grep -c -m 1 -F "FORMAT tuple('TSV')" "${metadata_file}"

metadata_file=$(grep -rl -F "URL 'http://example.test/m'" "${WORKING_DIR}" --include='*.sql')
sed -i "s|FORMAT 'TSV'|FORMAT 'TSV' HEADERS [('Authorization', 'token')]|" "${metadata_file}"
grep -c -m 1 -F "HEADERS [('Authorization', 'token')]" "${metadata_file}"

echo '--- the stored definition attaches ---'
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
DETACH DICTIONARY db.d;
ATTACH DICTIONARY db.d;
SELECT count() FROM system.tables WHERE database = 'db' AND name = 'd' AND position(create_table_query, 'tuple(') > 0;
DETACH DICTIONARY db.m;
ATTACH DICTIONARY db.m;
SELECT count() FROM system.tables WHERE database = 'db' AND name = 'm';
"

echo '--- the same value stated in a new definition is refused ---'
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" --send_logs_level fatal -q "
CREATE DICTIONARY db.d2 (id UInt64) PRIMARY KEY id
SOURCE(HTTP(URL 'http://example.test/' FORMAT tuple('TSV'))) LAYOUT(FLAT()) LIFETIME(0);" 2>&1 >/dev/null \
    | grep -o -m 1 -F 'does not accept a collection value'

rm -rf "${WORKING_DIR}"
