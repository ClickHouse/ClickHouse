#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not available in the fast-test build.

# `engine_file_allow_create_multiple_files` makes a writer pick the next free `data.<n>` name and
# append it to the storage's path list. Concurrent writers must not choose the same name, and the
# list must stay consistent with what is on disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WRITERS=16
TABLE="t_${CLICKHOUSE_DATABASE}"
OUT_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_streams"
trap 'rm -rf "${OUT_DIR:?}"; $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"' EXIT

rm -rf "${OUT_DIR:?}"
mkdir -p "$OUT_DIR"

# A File table created without a path gets its own directory under the server's data path, so this
# test needs no user_files handling and stays parallel-safe.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${TABLE} (x UInt64, y String) ENGINE = File(Parquet)"
# Parquet cannot be appended to, and the base file must be non-empty, or the extra-file branch is
# never reached.
$CLICKHOUSE_CLIENT -q "INSERT INTO ${TABLE} VALUES (0, 'base')"

# Settings go on the query: the test runner randomizes async_insert, and batching two inserts into
# one flush would leave a single writer.
pids=()
for i in $(seq 1 $WRITERS); do
    ${CLICKHOUSE_CURL} -X POST "${CLICKHOUSE_URL}" --data-binary "
        INSERT INTO ${TABLE}
        SETTINGS engine_file_allow_create_multiple_files = 1,
                 engine_file_truncate_on_insert = 0,
                 async_insert = 0
        VALUES ($i, 'w$i')" > "$OUT_DIR/w$i.out" 2> "$OUT_DIR/w$i.err" &
    pids+=("$!")
done
for pid in "${pids[@]}"; do
    wait "$pid" || echo "writer $pid did not finish"
done

# Without counting the writers that really ran, one dying early would still match the reference.
echo "writers $(find "$OUT_DIR" -name 'w*.out' | wc -l)"
# Errors are asserted rather than suppressed: two writers choosing one name surfaces as
# CANNOT_APPEND_TO_FILE, which the usual `2>/dev/null` idiom would hide.
echo "errors $(cat "$OUT_DIR"/w*.out "$OUT_DIR"/w*.err | grep -c 'DB::Exception')"
$CLICKHOUSE_CLIENT -q "SELECT 'rows', count() FROM ${TABLE}"
# A name appended twice is read once per copy, so it inflates the row count while leaving fewer
# distinct files behind.
$CLICKHOUSE_CLIENT -q "SELECT 'files', uniqExact(_file) FROM ${TABLE}"
