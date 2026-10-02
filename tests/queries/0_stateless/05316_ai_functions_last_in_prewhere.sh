#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An AI function issues one request per row. PREWHERE evaluates its conditions as read
# steps in order, each on the rows that passed the previous ones, so a condition with an
# AI function is moved to PREWHERE after all other conditions.
#
# `EXPLAIN` names the PREWHERE filter with its conditions in a canonical order, so the
# order is taken from the `Condition ... moved to PREWHERE` log lines of
# `MergeTreeWhereOptimizer`. Each moved condition is printed as `ai` or `cheap`.
#
# Only `EXPLAIN` is used, so no HTTP call is made.

CHAT_CREDS="ai_creds_${CLICKHOUSE_DATABASE}"
VEC_CREDS="ai_vec_creds_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE tab (flag UInt8, text String, other String) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO tab SELECT number % 2, 'row ' || toString(number), 'x' FROM numbers(16);
    CREATE NAMED COLLECTION ${CHAT_CREDS} AS provider = 'openai', endpoint = 'http://localhost:1/v1/chat/completions', model = 'test-model', api_key = 'test-key';
    CREATE NAMED COLLECTION ${VEC_CREDS} AS provider = 'openai', endpoint = 'http://localhost:1/v1/embeddings', api_key = 'test-key';
"

CHAT="map('credentials', '${CHAT_CREDS}')"
VEC="map('credentials', '${VEC_CREDS}')"

# Prints the conditions moved to PREWHERE, in order. The settings that decide whether and
# how conditions are moved are pinned rather than left to test randomization.
function moved()
{
    local label=$1 where=$2 allow_reorder=${3:-1}
    local order
    order=$($CLICKHOUSE_CLIENT -q "
        EXPLAIN SELECT other FROM tab WHERE ${where}
        SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_enable_optimizations = 1,
            query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1, allow_reorder_prewhere_conditions = ${allow_reorder},
            send_logs_level = 'test'" 2>&1 >/dev/null \
        | grep -F 'moved to PREWHERE' \
        | sed -E 's/.*Condition\(exp:(.*) viable: .*/\1/' \
        | while read -r condition; do if [[ $condition =~ ai[A-Z] ]]; then echo -n 'ai '; else echo -n 'cheap '; fi; done)
    echo "${label}: ${order% }"
}

moved 'aiGenerate'   "aiGenerate(text, ${CHAT}) != '' AND flag = 1"
moved 'aiClassify'   "aiClassify(text, ['a', 'b'], ${CHAT}) = 'a' AND flag = 1"
moved 'aiExtract'    "aiExtract(text, 'the topic', ${CHAT}) != '' AND flag = 1"
moved 'aiTranslate'  "aiTranslate(text, 'French', ${CHAT}) != '' AND flag = 1"
moved 'aiFilter'     "aiFilter(text, 'matches', ${CHAT}) AND flag = 1"
moved 'aiRedact'     "aiRedact(text, ['email'], ${CHAT}) != '' AND flag = 1"
moved 'aiEmbed'      "length(aiEmbed(text, 'test-model', ${VEC})) != 0 AND flag = 1"
moved 'aiSimilarity' "aiSimilarity(text, 'reference', 'test-model', ${VEC}) != 0 AND flag = 1"

# A lambda body is not among the condition's children, so the capture reports the trait.
moved 'AI inside a lambda' "arrayExists(x -> aiFilter(x, 'matches', ${CHAT}), [text]) AND flag = 1"

# Conditions on the same columns are grouped and moved together. The AI condition is kept
# out of the group, otherwise it would move ahead of `flag = 1` with `text LIKE`.
moved 'AI and a cheap condition on the same column' "aiFilter(text, 'matches', ${CHAT}) AND text LIKE '%1%' AND flag = 1"

# An `OR` is not split, so the whole disjunction is expensive and moved last.
moved 'AI inside OR' "(flag = 1 OR aiFilter(text, 'matches', ${CHAT})) AND text LIKE '%1%'"

# With no other condition the AI condition is still moved.
moved 'AI condition alone' "aiFilter(text, 'matches', ${CHAT})"

# Without reordering the written order is kept, so the AI condition stays in WHERE.
moved 'Reordering disabled' "aiFilter(text, 'matches', ${CHAT}) AND flag = 1" 0

$CLICKHOUSE_CLIENT -q "
    DROP NAMED COLLECTION ${CHAT_CREDS};
    DROP NAMED COLLECTION ${VEC_CREDS};
    DROP TABLE tab;
"
