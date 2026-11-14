#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: the dump of a `Replicated` database names macros that the replaying `clickhouse-local` does not have.

# --dump-schema emits `allow_experimental_shuffle_query` for a constraint whose subquery has `SHUFFLE`, which CREATE runs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

$CLICKHOUSE_CLIENT --allow_experimental_shuffle_query=1 -q "CREATE TABLE constraint_shuffle (x UInt64,
    CONSTRAINT c CHECK x < (SELECT max(number) FROM numbers(3) SHUFFLE)) ENGINE = Memory"
$CLICKHOUSE_CLIENT --dump-schema "${CLICKHOUSE_DATABASE}" > "$work/dump.sql" 2>"$work/err.txt" || echo "dump failed: $(cat "$work/err.txt")"
echo "shuffle gate: $(grep -c '^SET allow_experimental_shuffle_query = 1;' "$work/dump.sql")"
$CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/dump.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
echo "replay rc=${PIPESTATUS[0]}"
