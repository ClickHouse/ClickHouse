#!/usr/bin/env bash

# This test tests 2 things with and without the direct read optimization:
# 1. That short ngrams (parsing words shorter than the ngram length N) return an empty set when tokenized
# 2. That empty inputs to the search tokens return 0 rows instead of the whole table

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS=" "
cat <<EOF | $CLICKHOUSE_CLIENT -n $SETTINGS
DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt32,
    message String,
    arr Array(String),
    map Map(String, String),
    INDEX idx(message) TYPE text(tokenizer = ngrams(4)),
    INDEX array_idx(arr) TYPE text(tokenizer = ngrams(4)),
    INDEX map_keys_idx mapKeys(map) TYPE text(tokenizer = ngrams(4)),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab
VALUES
(1, 'abc', ['abc'], {'abc':'abc'}),
(2, 'def', ['def', 'abc'], {'abc':'foo', 'def':'foo'});
EOF

for direct_read_setting in 0 1; do
    echo "Setting query_plan_direct_read_from_text_index to $direct_read_setting."

    SETTINGS=" "
    SETTINGS="$SETTINGS --use_query_condition_cache=0 "
    SETTINGS="$SETTINGS --use_skip_indexes_on_data_read=1 "
    SETTINGS="$SETTINGS --query_plan_direct_read_from_text_index=$direct_read_setting "
    cat <<EOF | $CLICKHOUSE_CLIENT -n $SETTINGS
-- { echoOn }
SELECT count() FROM tab WHERE hasAnyTokens(message, ['abc']);
SELECT count() FROM tab WHERE hasAllTokens(message, ['abc']);

SELECT count() FROM tab WHERE hasAnyTokens(message, ['']);
SELECT count() FROM tab WHERE hasAllTokens(message, ['']);

SELECT count() FROM tab WHERE hasAnyTokens(message, []);
SELECT count() FROM tab WHERE hasAllTokens(message, []);

SELECT count() FROM tab WHERE hasAnyTokens(message, 'abc');
SELECT count() FROM tab WHERE hasAllTokens(message, 'abc');

SELECT count() FROM tab WHERE hasAnyTokens(message, '');
SELECT count() FROM tab WHERE hasAllTokens(message, '');

SELECT tokens('abc', 'ngrams', 4) AS tokens;

SELECT count() FROM tab WHERE hasToken(message, 'abc');

-- Arrays
SELECT count() FROM tab WHERE hasAnyTokens(arr, ['abc']);
SELECT count() FROM tab WHERE hasAllTokens(arr, ['abc']);

SELECT count() FROM tab WHERE hasAnyTokens(arr, ['']);
SELECT count() FROM tab WHERE hasAllTokens(arr, ['']);

SELECT count() FROM tab WHERE hasAnyTokens(arr, []);
SELECT count() FROM tab WHERE hasAllTokens(arr, []);

SELECT count() FROM tab WHERE hasAnyTokens(arr, 'abc');
SELECT count() FROM tab WHERE hasAllTokens(arr, 'abc');

SELECT count() FROM tab WHERE hasAnyTokens(arr, '');
SELECT count() FROM tab WHERE hasAllTokens(arr, '');

SELECT count() FROM tab WHERE has(arr, 'abc');
SELECT count() FROM tab WHERE has(arr, '');

-- Maps
SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(map), ['abc']);
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map), ['abc']);

SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(map), ['']);
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map), ['']);

SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(map), []);
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map), []);

SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(map), 'abc');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map), 'abc');

SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(map), '');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map), '');

SELECT count() FROM tab WHERE mapContains(map, 'abc');
SELECT count() FROM tab WHERE mapContains(map, '');

SELECT count() FROM tab WHERE has(map, 'abc');
SELECT count() FROM tab WHERE has(map, '');
-- { echoOff }
EOF

done

$CLICKHOUSE_CLIENT -q 'DROP TABLE tab;'
