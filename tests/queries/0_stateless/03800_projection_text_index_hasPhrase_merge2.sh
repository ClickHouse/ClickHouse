#!/usr/bin/env bash
# Verify hasPhrase correctness through projection text index merge with interleaved ids.
# Regression test for position data ordering bug when mapOffsets does non-trivial reordering.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_OPT="--allow_experimental_projection_text_index 1 ${CLICKHOUSE_CLIENT_OPT:-}"
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_phrase_merge_bug2"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_phrase_merge_bug2 (id UInt64, body String,
    PROJECTION idx_text INDEX body TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking',
        enable_phrase_query_support = 1))
ENGINE = MergeTree ORDER BY id"

cat "${CURDIR}/03800_projection_text_index_hasPhrase_merge2.part1.tsv" | ${CLICKHOUSE_CLIENT} -q "INSERT INTO t_phrase_merge_bug2 FORMAT TSV"
cat "${CURDIR}/03800_projection_text_index_hasPhrase_merge2.part2.tsv" | ${CLICKHOUSE_CLIENT} -q "INSERT INTO t_phrase_merge_bug2 FORMAT TSV"

${CLICKHOUSE_CLIENT} -q "SELECT 'before_optimize_hasPhrase', count() FROM t_phrase_merge_bug2 WHERE hasPhrase(body, 'United States') SETTINGS max_threads = 1"
${CLICKHOUSE_CLIENT} -q "SELECT 'before_optimize_hasAllTokens', count() FROM t_phrase_merge_bug2 WHERE hasAllTokens(body, 'United States') SETTINGS max_threads = 1"

${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_phrase_merge_bug2 FINAL"

${CLICKHOUSE_CLIENT} -q "SELECT sleepEachRow(0.1) FROM numbers(50)
WHERE (SELECT count() FROM system.parts WHERE active AND table = 't_phrase_merge_bug2' AND database = currentDatabase()) > 1
FORMAT Null"

${CLICKHOUSE_CLIENT} -q "SELECT 'after_optimize_hasPhrase', count() FROM t_phrase_merge_bug2 WHERE hasPhrase(body, 'United States') SETTINGS max_threads = 1"
${CLICKHOUSE_CLIENT} -q "SELECT 'after_optimize_hasAllTokens', count() FROM t_phrase_merge_bug2 WHERE hasAllTokens(body, 'United States') SETTINGS max_threads = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_phrase_merge_bug2"
