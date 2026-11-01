#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: relies on the local user_files directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `URL` engine dispatches non-HTTP schemes to a delegate (File/object storage) wrapped in
# `StorageURLSchemeDispatch`. The wrapper must forward the delegate's `supportsTrivialCountOptimization`
# contract: `StorageProxy` does not forward it, so without the override the wrapper falls back to the
# `IStorage` default (`false`), while `StorageFile`/`StorageObjectStorage`/plain `StorageURL` return
# `true`. The planner reads this bit before enabling the optimized count path, so `SELECT count()`
# from `ENGINE = URL('file://...')` would otherwise read and parse the external data instead of just
# counting rows.

# A file whose values are invalid for the declared type. The optimized count path only counts rows
# and never materializes the column, so it succeeds; a full read parses the column and fails. This
# makes the difference observable without depending on profiling/timing.
CSV="${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"
CSV_ABS="${USER_FILES_PATH}/${CSV}"
printf 'notnum\nfoo\nbar\n' > "$CSV_ABS"

echo "--- control: direct file() forwards the optimization, so count() does not parse the malformed column ---"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM file('${CSV}', CSV, 'x UInt32') SETTINGS optimize_trivial_count_query = 1, optimize_count_from_files = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_t"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t (x UInt32) ENGINE = URL('file://${CSV_ABS}', 'CSV')"

echo "--- ENGINE = URL('file://...') forwards supportsTrivialCountOptimization: count() uses the optimized path (=3) ---"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_t SETTINGS optimize_trivial_count_query = 1, optimize_count_from_files = 1"

echo "--- with the optimization disabled, the malformed column is read and parsing fails (documents the mechanism) ---"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_t SETTINGS optimize_trivial_count_query = 0, optimize_count_from_files = 0" 2>&1 \
    | grep -qiE "Cannot parse|Expected end of line|INCORRECT_DATA|CANNOT_PARSE" && echo "full-read-parses-and-fails" || echo "NO ERROR (unexpected)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t"
rm -f "$CSV_ABS"
