#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, long

# Per plan 02 §step 7-B lines 656-658: this is the acceptance gate for the
# legacy 02354_* → 03991_* rewrite. For each (id, _score) pair the legacy
# ORDER BY <distance>(vec, ref) LIMIT K pattern produces, the vectorSearch
# table function must produce a row-for-row identical set. Zero diffs is
# the pass criterion.
#
# The sweep restricts itself to legacy tests whose rewrite was a clean
# mechanical substitution (no EXPLAIN, no LIMIT > 1 with quantization-
# dependent ties, no rescoring, no settings that only apply to the legacy
# code path). The remaining legacy tests are either:
#   - DDL / format / cache (untouched in the 02354_* corpus), or
#   - Surfaced in the worker's final report as "non-mechanical / deferred".

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pairs of (table_setup, legacy_query, vectorSearch_query) live as
# heredocs below so that one binary failure surfaces with full context.

set -e

run_pair()
{
    local label="$1"
    local setup="$2"
    local legacy="$3"
    local newform="$4"

    local legacy_out
    local newform_out
    legacy_out=$(${CLICKHOUSE_CLIENT} --query="${setup} ${legacy}")
    newform_out=$(${CLICKHOUSE_CLIENT} --query="${setup} ${newform}")

    if [[ "${legacy_out}" != "${newform_out}" ]]
    then
        echo "DIFF in pair: ${label}"
        echo "-- legacy --"
        echo "${legacy_out}"
        echo "-- vectorSearch --"
        echo "${newform_out}"
        return 1
    fi
}

# Each pair runs against a fresh table to avoid cross-test interference.

SETUP_TAB="
DROP TABLE IF EXISTS sweep_tab;
CREATE TABLE sweep_tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO sweep_tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);
"

# Pair 1: basic top-K via L2Distance.
LEGACY_1="
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM sweep_tab
ORDER BY L2Distance(vec, reference_vec), id
LIMIT 3;
"
NEWFORM_1="
SET allow_experimental_search_topk_table_functions = 1;
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM vectorSearch(currentDatabase(), sweep_tab, idx, [0.0, 2.0], 3)
ORDER BY _score, id;
"
run_pair "basic_l2_top3" "${SETUP_TAB}" "${LEGACY_1}" "${NEWFORM_1}"

# Pair 2: detach/attach equivalence.
SETUP_DA="
DROP TABLE IF EXISTS sweep_da;
CREATE TABLE sweep_da(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO sweep_da VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);
DETACH TABLE sweep_da SYNC;
ATTACH TABLE sweep_da;
"
LEGACY_2="
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM sweep_da
ORDER BY L2Distance(vec, reference_vec), id
LIMIT 3;
"
NEWFORM_2="
SET allow_experimental_search_topk_table_functions = 1;
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM vectorSearch(currentDatabase(), sweep_da, idx, [0.0, 2.0], 3)
ORDER BY _score, id;
"
run_pair "detach_attach" "${SETUP_DA}" "${LEGACY_2}" "${NEWFORM_2}"

# Pair 3: cosineDistance.
SETUP_COS="
DROP TABLE IF EXISTS sweep_cos;
CREATE TABLE sweep_cos(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO sweep_cos VALUES (0, [1.0, 0.0]), (1, [1.0, 0.1]), (2, [0.0, 1.0]), (3, [0.1, 1.0]), (4, [0.5, 0.5]);
"
LEGACY_3="
WITH [1.0, 0.0] AS reference_vec
SELECT id, vec
FROM sweep_cos
ORDER BY cosineDistance(vec, reference_vec), id
LIMIT 3;
"
NEWFORM_3="
SET allow_experimental_search_topk_table_functions = 1;
SELECT id, vec
FROM vectorSearch(currentDatabase(), sweep_cos, idx, [1.0, 0.0], 3)
ORDER BY _score, id;
"
run_pair "cosine_top3" "${SETUP_COS}" "${LEGACY_3}" "${NEWFORM_3}"

# Pair 4: multi-mark / large dataset.
SETUP_MM="
DROP TABLE IF EXISTS sweep_mm;
CREATE TABLE sweep_mm(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;
INSERT INTO sweep_mm SELECT number, [toFloat32(number), 0.0] from numbers(10000);
"
LEGACY_4="
WITH [1.0, 0.0] AS reference_vec
SELECT id, vec
FROM sweep_mm
ORDER BY L2Distance(vec, reference_vec), id
LIMIT 1;
"
NEWFORM_4="
SET allow_experimental_search_topk_table_functions = 1;
SELECT id, vec
FROM vectorSearch(currentDatabase(), sweep_mm, idx, [1.0, 0.0], 1)
ORDER BY _score, id;
"
run_pair "multi_mark_top1" "${SETUP_MM}" "${LEGACY_4}" "${NEWFORM_4}"

# Cleanup
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sweep_tab; DROP TABLE IF EXISTS sweep_da; DROP TABLE IF EXISTS sweep_cos; DROP TABLE IF EXISTS sweep_mm;"

echo "OK"
