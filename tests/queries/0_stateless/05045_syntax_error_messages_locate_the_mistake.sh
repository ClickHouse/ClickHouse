#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The list of alternatives that the parser could accept is not interesting here and grows whenever a
# new statement is added to the grammar, so cut it off together with the version.
run()
{
    echo "--- $1"
    ${CLICKHOUSE_LOCAL} --implicit_select 0 --query "$1" 2>&1 \
        | sed -e 's/Expected one of: .*//' -e 's/Expected .*\.$//' -e 's/ (version [^)]*)//' -e 's/^Code: 62. DB::Exception: //' \
        | sed -e 's/[[:space:]]*$//'
}

echo '=== unclosed brackets: the error points at the place where the parser stopped, and every bracket'
echo '=== that is never closed is listed with its own position'

# The bracket of `count(` is the one the user forgot to close, but it gets matched with the `)` of the
# innermost subquery, so bracket counting blames the outermost bracket at position 15 instead.
run "SELECT a FROM (SELECT b FROM (SELECT c FROM (SELECT count(* AS cnt FROM numbers(10)) t3) t2) t1"
run "SELECT count(* FROM numbers(10)"
run "SELECT (1, 2"
run "SELECT [1, 2"
run "SELECT ((1)"
# Two brackets are left open at once.
run "SELECT x FROM (SELECT arrayMap(y -> y, [1]"

echo
echo '=== a closing bracket that does not close anything names what it fails to match'
run "SELECT 1)"
run "SELECT [1)"
run "SELECT (1]"

echo
echo '=== a mistyped keyword is named explicitly instead of being buried in the list of alternatives'
run "SELEgT 1"
run "ESLECT 1"
run "CREAT TABLE t (x UInt8) ENGINE = Memory"
run "SELECT 1 WHERE 1 ANF 2"

echo
echo '=== but a name that merely looks like a keyword is not reported as a typo'
run "SELECT 1 hits hits"
run "SELECT 1 orders orders"
# `an1` is two edits away from `ANY`, and a word with a digit in it is never a mistyped keyword.
run "SELECT 1 an1 an1"
