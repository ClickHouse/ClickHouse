#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs Parquet support.

# Regression test for a logical error 'KeyCondition uses PREWHERE output' in the
# Parquet v3 reader (src/Processors/Formats/Impl/Parquet/Reader.cpp). It fired when a
# format-level key condition referenced a column that is not read from the file: here
# `s` is used only inside `indexHint` (never as an output column, because the query is
# an aggregate), so it has no file-readable output column. Only reproduces with the old
# analyzer. https://github.com/ClickHouse/ClickHouse/pull/109267

# clickhouse-local with a per-test unique file so the test is safe to run in parallel
# with itself (the flaky check runs it many times concurrently).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
trap 'rm -f "${FILE}"' EXIT

${CLICKHOUSE_LOCAL} -q "INSERT INTO FUNCTION file('${FILE}') SELECT number::String AS s FROM numbers(10) SETTINGS engine_file_truncate_on_insert = 1;"

# A PREWHERE and a WHERE, each wrapping a condition on `s` in `indexHint`, with an
# aggregate so `s` is never read as an output column. Used to throw a logical error in
# the Parquet reader (aborting in debug/sanitizer builds). `s = 'x'` is outside the data
# range, so min/max pruning correctly removes every row group -> 0.
${CLICKHOUSE_LOCAL} -q "SELECT count() FROM file('${FILE}') PREWHERE indexHint(s = 'x') WHERE indexHint(s = 'y') SETTINGS enable_analyzer = 0;"

# The original AST-fuzzer shape: \`match\` with the column as the (non-constant) pattern.
${CLICKHOUSE_LOCAL} -q "SELECT count() FROM file('${FILE}') PREWHERE indexHint(match('x', s)) WHERE indexHint(match('y', s)) SETTINGS enable_analyzer = 0;"

# The analyzer accepts the same query and must keep returning correct results.
${CLICKHOUSE_LOCAL} -q "SELECT count() FROM file('${FILE}') PREWHERE indexHint(s = 'x') WHERE indexHint(s = 'y') SETTINGS enable_analyzer = 1;"
