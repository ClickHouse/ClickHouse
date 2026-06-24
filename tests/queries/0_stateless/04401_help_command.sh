#!/usr/bin/env bash
# Interactive `help`/`man` CLI command: render the embedded documentation of an entity from
# `system.documentation`, formatted from Markdown, in the terminal.
# https://github.com/ClickHouse/ClickHouse/issues/89377

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The output of `clickhouse-local -q` is a (non-tty) pipe, so the rendering is deterministic plain text.

echo "-- help with no argument prints usage"
$CLICKHOUSE_LOCAL -q "help" | grep -qF 'Usage: help <name>' && echo OK

echo "-- renders an entity (banner with name and type)"
$CLICKHOUSE_LOCAL -q "help max_threads" | grep -qxF 'max_threads (Setting)' && echo OK

echo "-- plain rendering has no ANSI escapes and no raw Markdown code fences"
out=$($CLICKHOUSE_LOCAL -q "help domainWithoutWWW")
printf '%s' "$out" | grep -qP '\x1b' && echo "FAIL: ANSI escapes in non-tty output" || echo OK
printf '%s' "$out" | grep -qF '```' && echo "FAIL: raw code fence in output" || echo OK

echo "-- identifiers with underscores are kept literal (not parsed as italics)"
$CLICKHOUSE_LOCAL -q "help max_threads" | grep -qF 'max_threads' && echo OK

echo "-- a SQL code block is rendered indented (with syntax highlighting on a terminal)"
$CLICKHOUSE_LOCAL -q "help domainWithoutWWW" | grep -qE '^    SELECT ' && echo OK

echo "-- lookup is case-insensitive"
$CLICKHOUSE_LOCAL -q "help MAX_THREADS" | grep -qxF 'max_threads (Setting)' && echo OK

echo "-- the forms help / man / /help / /man are equivalent"
ref=$($CLICKHOUSE_LOCAL -q "help max_threads" | md5sum)
for form in "man" "/help" "/man"; do
    got=$($CLICKHOUSE_LOCAL -q "$form max_threads" | md5sum)
    [ "$got" = "$ref" ] && echo OK || echo "FAIL: form '$form' differs from 'help'"
done

echo "-- a word matching several entities renders all of them"
out=$($CLICKHOUSE_LOCAL -q "help file")
echo "$out" | grep -qF '(Function)' && echo OK
echo "$out" | grep -qF '(Table Engine)' && echo OK

echo "-- a typo offers similar names (by edit distance)"
out=$($CLICKHOUSE_LOCAL -q "help maxx_threads")
echo "$out" | grep -qF "No documentation found for 'maxx_threads'." && echo OK
echo "$out" | grep -qF 'Maybe you meant:' && echo OK
echo "$out" | grep -qF 'max_threads' && echo OK

echo "-- a word only present in documentation text is suggested by content"
out=$($CLICKHOUSE_LOCAL -q "help HyperThreading")
echo "$out" | grep -qF "No documentation found for 'HyperThreading'." && echo OK
echo "$out" | grep -qF 'Found in the documentation of:' && echo OK

echo "-- a completely unknown word finds nothing"
$CLICKHOUSE_LOCAL -q "help zzqqxx_no_such_entity" | grep -qF 'Nothing similar found.' && echo OK

echo "-- 'help' is still parsed as SQL in clickhouse-client batch mode (not a meta-command)"
$CLICKHOUSE_CLIENT -q "help max_threads" 2>&1 | grep -qF 'SYNTAX_ERROR' && echo OK
