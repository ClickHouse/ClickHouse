#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A part may hold the files of a text index without listing them in `checksums.txt`, e.g. when
# they were left behind by a mutation (see https://github.com/ClickHouse/ClickHouse/pull/109616).
# Such an index is not readable, and the search predicate has to be computed from the column data,
# as in a part where the index was never materialized. Direct read from the text index used to fail
# on such parts with `UNKNOWN_IDENTIFIER`: the index was considered materialized when the columns
# to read were chosen, so the source column was not read, but the index reader was not created
# because the index files are not in the checksums, and the main reader had to evaluate the
# predicate on a block without the source column.
#
# `clickhouse-local` is used because the test copies the index files from a part of one table into
# a part of another table, where they are not in the checksums.

data_path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${data_path}"

index_definition="INDEX idx s TYPE text(tokenizer = splitByString([' ']), preprocessor = lower(s)) GRANULARITY 1"

part_paths=$(${CLICKHOUSE_LOCAL} --path "${data_path}" -m -q "
CREATE TABLE t_with_index (k UInt64, s String, ${index_definition}) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_orphan_index (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_with_index SELECT number, if(number % 10 = 0, 'Foo bar', 'baz qux') FROM numbers(1000);
INSERT INTO t_orphan_index SELECT number, if(number % 10 = 0, 'Foo bar', 'baz qux') FROM numbers(1000);
SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_with_index' AND active;
SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_orphan_index' AND active;
")

part_with_index=$(echo "${part_paths}" | sed -n 1p)
part_orphan_index=$(echo "${part_paths}" | sed -n 2p)

cp "${part_with_index}"skp_idx_idx.* "${part_orphan_index}"

# The second insert creates a part with a properly materialized index, so the planner rewrites the
# predicate into a direct read from the index. The first part has orphan index files, so there the
# predicate is computed from the column data.
${CLICKHOUSE_LOCAL} --path "${data_path}" -m -q "
ALTER TABLE t_orphan_index ADD ${index_definition};
INSERT INTO t_orphan_index SELECT number + 1000, if(number % 10 = 0, 'Foo bar', 'baz qux') FROM numbers(1000);

SET use_skip_indexes_on_data_read = 1;
SET query_plan_optimize_count_from_text_index = 0;

SELECT 'direct read:', count() FROM t_orphan_index WHERE hasAnyTokens(s, ['foo']) SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT 'no direct read:', count() FROM t_orphan_index WHERE hasAnyTokens(s, ['foo']) SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'direct read with other columns:', count(), sum(k) FROM t_orphan_index WHERE hasAnyTokens(s, ['foo']) OR k = 1 SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT 'no direct read with other columns:', count(), sum(k) FROM t_orphan_index WHERE hasAnyTokens(s, ['foo']) OR k = 1 SETTINGS query_plan_direct_read_from_text_index = 0;
"

rm -rf "${data_path}"
