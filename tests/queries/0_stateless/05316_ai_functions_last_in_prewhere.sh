#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An AI function issues one request per row. PREWHERE evaluates its conditions as read
# steps in order, each on the rows that passed the previous ones, so a condition with an
# AI function is moved to PREWHERE after all other conditions.
#
# The order is taken from the PREWHERE filter column of `EXPLAIN json = 1`, which lists the
# conditions as `and(...)` arguments in their final order. The AI condition is last if the
# AI function appears after every cheap condition (`flag = 1`, `text LIKE`).
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

# Offset of the last match of a pattern in a string, empty if there is none.
function last_offset()
{
    grep -boE "$2" <<< "$1" | tail -1 | cut -d: -f1
}

# Prints whether the AI condition is in PREWHERE and whether it comes after the cheap ones.
# The settings that decide whether and how conditions are moved are pinned rather than left
# to test randomization.
function ai_position()
{
    local label=$1 where=$2 allow_reorder=${3:-1}
    local column
    column=$($CLICKHOUSE_CLIENT -q "
        EXPLAIN json = 1, actions = 1 SELECT other FROM tab WHERE ${where}
        SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_enable_optimizations = 1,
            query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1, allow_reorder_prewhere_conditions = ${allow_reorder}" \
        | grep -oE '"Prewhere filter column": "[^"]*"')

    local ai cheap
    ai=$(grep -boE 'ai[A-Z]' <<< "$column" | head -1 | cut -d: -f1)
    if [[ -z $ai ]]; then echo "${label}: not in PREWHERE"; return; fi
    for pattern in 'equals\(__table1\.flag' 'like\(__table1\.text'; do
        cheap=$(last_offset "$column" "$pattern")
        if [[ -n $cheap ]] && (( cheap > ai )); then echo "${label}: in PREWHERE, not last"; return; fi
    done
    echo "${label}: in PREWHERE, last"
}

ai_position 'aiGenerate'   "aiGenerate(text, ${CHAT}) != '' AND flag = 1"
ai_position 'aiClassify'   "aiClassify(text, ['a', 'b'], ${CHAT}) = 'a' AND flag = 1"
ai_position 'aiExtract'    "aiExtract(text, 'the topic', ${CHAT}) != '' AND flag = 1"
ai_position 'aiTranslate'  "aiTranslate(text, 'French', ${CHAT}) != '' AND flag = 1"
ai_position 'aiFilter'     "aiFilter(text, 'matches', ${CHAT}) AND flag = 1"
ai_position 'aiRedact'     "aiRedact(text, ['email'], ${CHAT}) != '' AND flag = 1"
ai_position 'aiEmbed'      "length(aiEmbed(text, 'test-model', ${VEC})) != 0 AND flag = 1"
ai_position 'aiSimilarity' "aiSimilarity(text, 'reference', 'test-model', ${VEC}) != 0 AND flag = 1"

# A lambda body is not among the condition's children, so the capture reports the trait.
ai_position 'AI inside a lambda' "arrayExists(x -> aiFilter(x, 'matches', ${CHAT}), [text]) AND flag = 1"

# Conditions on the same columns are grouped and moved together. The AI condition is kept
# out of the group, otherwise it would move ahead of `flag = 1` with `text LIKE`.
ai_position 'AI and a cheap condition on the same column' "aiFilter(text, 'matches', ${CHAT}) AND text LIKE '%1%' AND flag = 1"

# An `OR` is not split, so the whole disjunction is expensive and moved last.
ai_position 'AI inside OR' "(flag = 1 OR aiFilter(text, 'matches', ${CHAT})) AND text LIKE '%1%'"

# With no other condition the AI condition is still moved.
ai_position 'AI condition alone' "aiFilter(text, 'matches', ${CHAT})"

# Without reordering the written order is kept, the AI condition included.
ai_position 'Reordering disabled, AI written first' "aiFilter(text, 'matches', ${CHAT}) AND flag = 1" 0
ai_position 'Reordering disabled, AI written last' "flag = 1 AND aiFilter(text, 'matches', ${CHAT})" 0

$CLICKHOUSE_CLIENT -q "
    DROP NAMED COLLECTION ${CHAT_CREDS};
    DROP NAMED COLLECTION ${VEC_CREDS};
    DROP TABLE tab;
"
