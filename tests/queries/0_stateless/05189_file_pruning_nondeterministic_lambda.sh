#!/usr/bin/env bash
# Tags: no-fasttest

# The filter of a `file()` query is also used to decide which files to read at all, so a predicate
# that hides a non-deterministic call inside a lambda body must not be handed to that pruning: there
# it is drawn once per file and wipes out whole files, on top of the per-row draw that follows.
# The lambda captures nothing, so it is folded into a constant `ColumnFunction` and is not a
# `FUNCTION` node of the filter DAG, which is what the determinism guard has to see through.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"/
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"/*

# 16 files of 2000 rows. A predicate drawn per row keeps about half of the 32000 rows; a predicate
# also drawn per file keeps about half of the files first, so about a quarter of the rows. The
# assertion is one-sided at 45%: the per-row verdict misses it by about 130 standard deviations,
# while the per-file one would need 15 of the 16 files to survive their draw.
for i in $(seq 1 16); do
    seq 0 1999 > "${CLICKHOUSE_USER_FILES_UNIQUE}"/data$i.csv
done

GLOB="${CLICKHOUSE_TEST_UNIQUE_NAME}/data*.csv"

${CLICKHOUSE_CLIENT} -q "SELECT 'total', count() FROM file('${GLOB}', 'CSV', 'a UInt32')"

${CLICKHOUSE_CLIENT} -q "SELECT 'lambda predicate is not used to prune files',
    count() > 0.45 * 32000 FROM file('${GLOB}', 'CSV', 'a UInt32')
    WHERE arrayExists(x -> rand(x) % 2 = 0, materialize([1]))"

# Control: the same oracle with the call in plain sight, which the guard already rejected.
${CLICKHOUSE_CLIENT} -q "SELECT 'visible predicate is not used to prune files',
    count() > 0.45 * 32000 FROM file('${GLOB}', 'CSV', 'a UInt32')
    WHERE rand() % 2 = 0"

# Control: a deterministic predicate over `_file` is still used for pruning, so the guard did not
# degrade into refusing every predicate.
${CLICKHOUSE_CLIENT} -q "SELECT 'deterministic predicate still prunes', count()
    FROM file('${GLOB}', 'CSV', 'a UInt32') WHERE _file = 'data1.csv'"

rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"/*
