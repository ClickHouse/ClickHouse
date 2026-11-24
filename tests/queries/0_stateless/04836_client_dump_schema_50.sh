#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema reports a named collection that a dictionary's `CLICKHOUSE` source reads only through a query construction
# setting, of the source query or of the dictionary, which the source runs as part of its query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

$CLICKHOUSE_LOCAL --path "$work/src" -n -q "
CREATE NAMED COLLECTION ncq AS url = 'file:///dev/null', format = 'TSV', structure = 'id UInt64';
CREATE NAMED COLLECTION ncs AS url = 'file:///dev/null', format = 'TSV', structure = 'id UInt64';
CREATE DATABASE d;
CREATE DICTIONARY d.dq (id UInt64, v UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(QUERY 'SELECT toUInt64(0) AS id, toUInt64(0) AS v SETTINGS filter = ''id IN (SELECT id FROM url(ncq))'''))
    LAYOUT(FLAT()) LIFETIME(0);
CREATE DICTIONARY d.ds (id UInt64, v UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(QUERY 'SELECT toUInt64(0) AS id, toUInt64(0) AS v')) LAYOUT(FLAT()) LIFETIME(0)
    SETTINGS(filter = 'id IN (SELECT id FROM url(ncs))');
"

$CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > "$work/dump.sql" 2> "$work/err"
echo "dump rc=$?"
echo "query setting warned: $(grep -c 'd.dq depends on named collection ncq,' "$work/err")"
echo "dictionary setting warned: $(grep -c 'd.ds depends on named collection ncs,' "$work/err")"
echo "unscannable warned: $(grep -c 'could not be parsed here' "$work/err")"
