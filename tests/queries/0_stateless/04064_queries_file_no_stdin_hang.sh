#!/usr/bin/env bash
# Regression test for hanging when --queries-file has INSERT with inline data
# and stdin is an open pipe with no data/EOF.
# https://github.com/ClickHouse/ClickHouse/pull/96494

# Random settings limits: send_table_structure_on_insert_with_inline_data=(1, 1)
# With `send_table_structure_on_insert_with_inline_data = 0` the client would route
# the default (non `--inline-insert-data`) INSERT cases below into the inline insert
# data branch, so the legacy `sendData` appending-stdin path would not be exercised.
# Pin the setting so the default cases deterministically cover the legacy path, while
# the `--inline-insert-data` cases cover the inline branch.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERIES_FILE="${CLICKHOUSE_TMP}/04064_queries_$$.sql"

cat > "$QUERIES_FILE" <<'EOF'
CREATE TABLE IF NOT EXISTS test_04064 (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO test_04064 VALUES (1), (2), (3);
SELECT sum(x) FROM test_04064;
DROP TABLE test_04064;
EOF

# Run with stdin as an open pipe (no data, no EOF). Inline INSERT data is the complete
# payload, so an unused inherited stdin must not prevent it from executing. Use a FIFO
# so stdin never gets EOF.
FIFO="${CLICKHOUSE_TMP}/04064_fifo_$$"
mkfifo "$FIFO"

# Open the FIFO for reading in background to keep it alive (no EOF).
# Use fd 4 to avoid conflicting with BASH_XTRACEFD (which uses fd 3).
exec 4<>"$FIFO"

run_unused_stdin_test()
{
    local expected_output="$1"
    shift
    local output_file="${CLICKHOUSE_TMP}/04064_empty_stdin_$$.out"

    if ! timeout 30 "$@" <&4 > "$output_file" 2>&1
    then
        echo "Expected INSERT with an unused inherited stdin to succeed" >&2
        cat "$output_file" >&2
        exit 1
    fi

    grep -qx "$expected_output" "$output_file" ||
    {
        cat "$output_file" >&2
        exit 1
    }

    rm -f "$output_file"
}

run_ambiguous_infile_stdin_test()
{
    local output_file="${CLICKHOUSE_TMP}/04064_ambiguous_infile_stdin_$$.out"

    if timeout 30 "$@" <&4 > "$output_file" 2>&1
    then
        echo "Expected INSERT FROM INFILE with an open inherited stdin to fail" >&2
        cat "$output_file" >&2
        exit 1
    fi

    grep -Fq 'Processing INSERT with inline data or infile and an open stdin without data or EOF is not supported' "$output_file" ||
    {
        cat "$output_file" >&2
        exit 1
    }

    rm -f "$output_file"
}

run_unused_stdin_test 6 $CLICKHOUSE_CLIENT --queries-file="$QUERIES_FILE"

# Also test with async_insert enabled — the async insert path has its own
# stdin check that must ignore an unused inherited pipe without hanging.
QUERIES_FILE_ASYNC="${CLICKHOUSE_TMP}/04064_queries_async_$$.sql"
cat > "$QUERIES_FILE_ASYNC" <<EOF
CREATE TABLE IF NOT EXISTS test_04064_async (x UInt32) ENGINE = MergeTree ORDER BY x;
SET async_insert = 1;
SET wait_for_async_insert = 1;
INSERT INTO test_04064_async VALUES (10), (20), (30);
SELECT sum(x) FROM test_04064_async;
DROP TABLE test_04064_async;
EOF

run_unused_stdin_test 60 $CLICKHOUSE_CLIENT --queries-file="$QUERIES_FILE_ASYNC"

# Also test with --inline-insert-data — this path has its own stdin check
# (in `is_inline_insert_data` branch) that must ignore an unused pipe without hanging.
QUERIES_FILE_INLINE="${CLICKHOUSE_TMP}/04064_queries_inline_$$.sql"
cat > "$QUERIES_FILE_INLINE" <<'EOF'
CREATE TABLE IF NOT EXISTS test_04064_inline (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO test_04064_inline VALUES (100), (200), (300);
SELECT sum(x) FROM test_04064_inline;
DROP TABLE test_04064_inline;
EOF

run_unused_stdin_test 600 $CLICKHOUSE_CLIENT --inline-insert-data --queries-file="$QUERIES_FILE_INLINE"

# Also test the `-q` / `--query` entrypoint with the same open-empty-pipe stdin.
# The parser/entrypoint differs from `--queries-file`, so cover it explicitly to
# guard against regressions in either CLI mode.
QUERY_Q="CREATE TABLE IF NOT EXISTS test_04064_q (x UInt32) ENGINE = MergeTree ORDER BY x; INSERT INTO test_04064_q VALUES (1000), (2000), (3000); SELECT sum(x) FROM test_04064_q; DROP TABLE test_04064_q;"

run_unused_stdin_test 6000 $CLICKHOUSE_CLIENT -q "$QUERY_Q"

QUERY_Q_ASYNC="CREATE TABLE IF NOT EXISTS test_04064_q_async (x UInt32) ENGINE = MergeTree ORDER BY x; SET async_insert = 1; SET wait_for_async_insert = 1; INSERT INTO test_04064_q_async VALUES (10000), (20000), (30000); SELECT sum(x) FROM test_04064_q_async; DROP TABLE test_04064_q_async;"

run_unused_stdin_test 60000 $CLICKHOUSE_CLIENT -q "$QUERY_Q_ASYNC"

QUERY_Q_INLINE="CREATE TABLE IF NOT EXISTS test_04064_q_inline (x UInt32) ENGINE = MergeTree ORDER BY x; INSERT INTO test_04064_q_inline VALUES (100000), (200000), (300000); SELECT sum(x) FROM test_04064_q_inline; DROP TABLE test_04064_q_inline;"

run_unused_stdin_test 600000 $CLICKHOUSE_CLIENT --inline-insert-data -q "$QUERY_Q_INLINE"

# `FROM INFILE` provides the INSERT payload, but an open stdin with neither data nor
# EOF is ambiguous: it could receive delayed data that cannot be appended safely.
# Verify that both query entrypoints reject this case promptly instead of blocking.
INFILE_DATA="${CLICKHOUSE_TMP}/04064_infile_data_$$.values"
printf '(42)\n' > "$INFILE_DATA"

$CLICKHOUSE_CLIENT -q "CREATE TABLE IF NOT EXISTS test_04064_infile (x UInt32) ENGINE = MergeTree ORDER BY x"
QUERIES_FILE_INFILE="${CLICKHOUSE_TMP}/04064_queries_infile_$$.sql"
printf "INSERT INTO test_04064_infile FROM INFILE '%s' FORMAT Values;\n" "$INFILE_DATA" > "$QUERIES_FILE_INFILE"

run_ambiguous_infile_stdin_test $CLICKHOUSE_CLIENT --queries-file="$QUERIES_FILE_INFILE"
run_ambiguous_infile_stdin_test $CLICKHOUSE_CLIENT -q "INSERT INTO test_04064_infile FROM INFILE '$INFILE_DATA' FORMAT Values"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_04064_infile"

exec 4>&-
rm -f "$FIFO" "$QUERIES_FILE" "$QUERIES_FILE_ASYNC" "$QUERIES_FILE_INLINE" "$QUERIES_FILE_INFILE" "$INFILE_DATA"
