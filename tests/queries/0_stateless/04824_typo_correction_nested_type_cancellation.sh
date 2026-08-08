#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fixture needs a deeply nested type, which makes the DDL large.

# Typo correction for an unresolved identifier walks every subcolumn of every candidate column.
# The substream tree doubles per nesting level, and the walk observed no cancellation, so a query
# on a deeply nested type ignored `max_execution_time` while it was still being analyzed.
# https://github.com/ClickHouse/ClickHouse/pull/86768#issuecomment-5224028011

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The server echoes the exception, hint text included, as a log line at the default level, which
# would match the greps below a second time.
CLICKHOUSE_CLIENT=${CLICKHOUSE_CLIENT/--send_logs_level=$CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL/--send_logs_level=none}

nested="UInt8"
for _ in $(seq 1 12); do
    nested="Array(Map(String, Tuple(a ${nested}, b ${nested})))"
done

# The type string is hundreds of kilobytes, so feed the DDL on stdin rather than as an argument.
echo "CREATE TABLE t_04824 (c ${nested}, plain UInt8) ENGINE = Memory" \
    | $CLICKHOUSE_CLIENT --max_query_size=1000000000

# A one-part identifier cannot match a compound subcolumn at any depth, so none of the four walks
# per column can contribute a hint. They ran anyway, taking 44 s on a debug build and 348 s under a
# sanitizer. The limit is far above what the query now costs, so this is not a timing race: it
# reports UNKNOWN_IDENTIFIER rather than TIMEOUT_EXCEEDED.
$CLICKHOUSE_CLIENT -q "SELECT nosuchcolumn FROM t_04824 SETTINGS max_execution_time = 10" 2>&1 \
    | grep -c "UNKNOWN_IDENTIFIER"

# A two-part identifier keeps the one walk that can contribute a hint, which is still expensive on
# a type this deep, so that walk must observe cancellation. Before the fix the query ran to
# completion and reported UNKNOWN_IDENTIFIER, ignoring the limit entirely.
$CLICKHOUSE_CLIENT -q "SELECT a.nosuchcolumn FROM t_04824 AS a SETTINGS max_execution_time = 0.001" 2>&1 \
    | grep -c "TIMEOUT_EXCEEDED"

# The hints themselves must not change. This one can only come from the walk: an alias expression
# has no column list to draw suggestions from, unlike a table, whose column map already holds
# subcolumn names and so keeps producing them either way.
$CLICKHOUSE_CLIENT -q "SELECT x.ab FROM (SELECT (1, 2)::Tuple(aa UInt8, bb UInt8) AS x)" 2>&1 \
    | grep -o "Maybe you meant: \['x.bb'\]"

# A one-part identifier must still be answered from the plain column names.
$CLICKHOUSE_CLIENT -q "SELECT plai FROM t_04824" 2>&1 | grep -o "Maybe you meant: \['plain'\]"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04824"
