#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `MySQLDump` parser maps the dump's columns onto the destination by name when the dump provides
# column names (here: the column list of the `INSERT` query) and `input_format_mysql_dump_map_column_names`
# is enabled (the default), so the schema-mismatch diagnostic must compare the inferred structure by
# name there too: a legally reordered column list with a plain value error must not pick up a bogus
# structure-mismatch explanation. Without column names, or with the setting disabled, the parser maps
# columns positionally and the diagnostic must follow.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- reordered column list, only a value error (1.5 into UInt8): no false positive"
{
    echo "CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump"
    echo "INSERT INTO t (b, a) VALUES ('ok', 1.5);"
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- reordered column list, text into a numeric column: a genuine structure mismatch"
{
    echo "CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump"
    echo "INSERT INTO t (b, a) VALUES ('ok', 'text');"
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- map_columns disabled: the parser maps positionally, so the reordered data is a genuine mismatch"
{
    echo "CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t SETTINGS input_format_mysql_dump_map_column_names = 0 FORMAT MySQLDump"
    echo "INSERT INTO t (b, a) VALUES ('ok', 1.5);"
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- no column list in the dump: the parser maps positionally even with map_columns enabled"
{
    echo "CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump"
    echo "INSERT INTO t VALUES ('ok', 1.5);"
} | $CLICKHOUSE_LOCAL 2>&1 | check
