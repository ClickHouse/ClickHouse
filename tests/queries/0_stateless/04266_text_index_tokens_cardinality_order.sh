#!/usr/bin/env bash
# Verifies that, after the first data part is analyzed, subsequent parts read text-index tokens
# in order of increasing cardinality (rarest first) rather than alphabetically.
# See PR https://github.com/ClickHouse/ClickHouse/pull/98226.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_text_index_tokens_order;

CREATE TABLE t_text_index_tokens_order
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;
"

# Insert three separate parts so the cardinality cache is populated after the first part.
# In each part:
#   'aaaa' is in every row     -> highest density
#   'zzzz' is in half the rows -> medium density
#   'mmmm' is in one row only  -> lowest density
# Alphabetical order: aaaa, mmmm, zzzz
# Cardinality order (rare first): mmmm, zzzz, aaaa
for i in 0 1 2; do
    ${CLICKHOUSE_CLIENT} -q "
INSERT INTO t_text_index_tokens_order
SELECT
    ($i * 100) + number,
    concat('aaaa', if(number = 0, ' mmmm', ''), if(number % 2 = 0, ' zzzz', ''))
FROM numbers(100);
"
done

${CLICKHOUSE_CLIENT} -q "
SELECT count() FROM t_text_index_tokens_order
WHERE hasAllTokens(s, ['aaaa', 'mmmm', 'zzzz']);
"

# Capture the 'Reading tokens ... from part ...' log line for each part.
# With max_threads = 1 the parts are processed in order, so the first line uses the
# alphabetical fallback (empty cardinality cache) and the next two use cardinality order.
${CLICKHOUSE_CLIENT} --send_logs_level=test -q "
SELECT count() FROM t_text_index_tokens_order
WHERE hasAllTokens(s, ['aaaa', 'mmmm', 'zzzz'])
SETTINGS max_threads = 1, use_text_index_tokens_cache = 0;
" 2>&1 \
    | grep "MergeTreeIndexGranuleText: Reading tokens" \
    | sed -E 's|^.*Reading tokens (\[[^]]*\]) from part .*/(all_[^/]+)/?$|Reading tokens \1 from part \2|'

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_text_index_tokens_order;"
