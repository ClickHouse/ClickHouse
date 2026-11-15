#!/usr/bin/env bash
# Test the `ls` command in clickhouse-local.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

F1="ls_test_file_.1"
F2="ls_test_file_.2"
F3="ls_test_file_.3"

TESTDIR="$CURDIR/ls_test_dir_$$"

cleanup() {
    cd "$CURDIR" || exit 1
    rm -rf "$TESTDIR"
}
trap cleanup EXIT

rm -rf "$TESTDIR"
mkdir "$TESTDIR"

echo "-- prepare files"
touch "$TESTDIR/$F1" "$TESTDIR/$F2" "$TESTDIR/$F3"

echo "-- ls"
(
    cd "$TESTDIR" || exit 1
    OUT=$($CLICKHOUSE_LOCAL -q "ls")
    echo "$OUT" | grep -Fx "$F1"
    echo "$OUT" | grep -Fx "$F2"
    echo "$OUT" | grep -Fx "$F3"
)
echo "OK"

echo "-- ls;"
(
    cd "$TESTDIR" || exit 1
    OUT=$($CLICKHOUSE_LOCAL -q "ls;")
    echo "$OUT" | grep -Fx "$F1"
    echo "$OUT" | grep -Fx "$F2"
    echo "$OUT" | grep -Fx "$F3"
)
echo "OK"

echo "-- ls x"
(
    cd "$TESTDIR" || exit 1
    $CLICKHOUSE_LOCAL -q "ls x" 2>&1 || true
) | grep -F 'Unknown expression identifier `ls`' > /dev/null
echo "OK"