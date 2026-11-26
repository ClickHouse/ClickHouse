-- Builds text indexes of parts with large vocabularies: once the token map outgrows the CPU cache,
-- the tokens of many rows are inserted together with prefetching.
-- Checks that the dictionaries are sorted and that the posting lists match the rows.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;

-- Large parts, so that the token maps outgrow the cache.
SET max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

DROP TABLE IF EXISTS tab_array;
DROP TABLE IF EXISTS tab_string;
DROP TABLE IF EXISTS tab_map;

SELECT '-- Array columns with tokens that sort in tricky ways';

CREATE TABLE tab_array
(
    id UInt64,
    arr Array(String),
    INDEX idx arr TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_array SELECT
    number,
    [
        concat(repeat('p', 70), toString(number % 5000)), -- A common prefix longer than 64 bytes.
        concat(repeat('q', 16), char(number % 4), toString(number % 300)), -- Zero and small bytes right after two chunks.
        concat('ab', repeat(char(0), number % 3)), -- Tokens that differ only in trailing zero bytes.
        concat(char(128 + number % 128), toString(number % 1000)), -- Bytes above 0x7F, which compare as unsigned.
        substring('abcdefghijklmnopqrstuvwxyz', 1, 1 + number % 26), -- Tokens that are prefixes of each other.
        toString(cityHash64(number) % 100000) -- A large vocabulary.
    ]
FROM numbers(100000);

SELECT countIf(tokens != arraySort(tokens))
FROM (SELECT part_name, groupArray(token) AS tokens FROM mergeTreeTextIndex(currentDatabase(), tab_array, idx) GROUP BY part_name)
SETTINGS max_threads = 1;

WITH
    index_rows AS (SELECT token, sum(cardinality) AS num_rows FROM mergeTreeTextIndex(currentDatabase(), tab_array, idx) GROUP BY token),
    data_rows AS (SELECT token, count() AS num_rows FROM (SELECT DISTINCT id, arrayJoin(arr) AS token FROM tab_array) GROUP BY token)
SELECT count() FROM ((SELECT * FROM index_rows EXCEPT SELECT * FROM data_rows) UNION ALL (SELECT * FROM data_rows EXCEPT SELECT * FROM index_rows));

SELECT '-- String columns';

CREATE TABLE tab_string
(
    id UInt64,
    s Nullable(String),
    t String,
    u String,
    v String,
    INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_t t TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_u u TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(u IN ('w1', 'w2'), '', u)),
    INDEX idx_v v TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

-- A large vocabulary, frequent tokens and unique tokens, both repeated in the row.
INSERT INTO tab_string SELECT number, if(number % 10 = 0, NULL, text), text, text, text
FROM
(
    SELECT
        number,
        concat('w', toString(cityHash64(number) % 100000), ' w', toString(number % 1000), ' r', toString(number), ' w', toString(number % 1000), ' r', toString(number)) AS text
    FROM numbers(100000)
);

SELECT countIf(tokens != arraySort(tokens))
FROM (SELECT part_name, groupArray(token) AS tokens FROM mergeTreeTextIndex(currentDatabase(), tab_string, idx_t) GROUP BY part_name)
SETTINGS max_threads = 1;

WITH
    index_rows AS (SELECT token, sum(cardinality) AS num_rows FROM mergeTreeTextIndex(currentDatabase(), tab_string, idx_s) GROUP BY token),
    data_rows AS (SELECT token, count() AS num_rows FROM (SELECT arrayJoin(arrayDistinct(tokens(assumeNotNull(s), 'splitByNonAlpha'))) AS token FROM tab_string WHERE s IS NOT NULL) GROUP BY token)
SELECT count() FROM ((SELECT * FROM index_rows EXCEPT SELECT * FROM data_rows) UNION ALL (SELECT * FROM data_rows EXCEPT SELECT * FROM index_rows));

WITH
    index_rows AS (SELECT token, sum(cardinality) AS num_rows FROM mergeTreeTextIndex(currentDatabase(), tab_string, idx_t) GROUP BY token),
    data_rows AS (SELECT token, count() AS num_rows FROM (SELECT arrayJoin(arrayDistinct(tokens(t, 'splitByNonAlpha'))) AS token FROM tab_string) GROUP BY token)
SELECT count() FROM ((SELECT * FROM index_rows EXCEPT SELECT * FROM data_rows) UNION ALL (SELECT * FROM data_rows EXCEPT SELECT * FROM index_rows));

WITH
    index_rows AS (SELECT token, sum(cardinality) AS num_rows FROM mergeTreeTextIndex(currentDatabase(), tab_string, idx_u) GROUP BY token),
    data_rows AS (SELECT token, count() AS num_rows FROM (SELECT arrayJoin(arrayDistinct(tokens(u, 'splitByNonAlpha'))) AS token FROM tab_string) WHERE token NOT IN ('w1', 'w2') GROUP BY token)
SELECT count() FROM ((SELECT * FROM index_rows EXCEPT SELECT * FROM data_rows) UNION ALL (SELECT * FROM data_rows EXCEPT SELECT * FROM index_rows));

-- The row ids, read directly from the index, match the rows.
SET use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT count(), sum(id) FROM tab_string WHERE hasAnyTokens(t, ['w0', 'w7', 'w999', 'w12345', 'w99999', 'r0', 'r777', 'r99999']);
SELECT count(), sum(id) FROM tab_string WHERE hasAnyTokens(t, ['w0', 'w7', 'w999', 'w12345', 'w99999', 'r0', 'r777', 'r99999']) SETTINGS use_skip_indexes = 0;

SELECT count(), sum(id) FROM tab_string WHERE hasAnyTokens(s, ['w0', 'w7', 'w999', 'w12345', 'w99999', 'r0', 'r777', 'r99999']);
SELECT count(), sum(id) FROM tab_string WHERE hasAnyTokens(s, ['w0', 'w7', 'w999', 'w12345', 'w99999', 'r0', 'r777', 'r99999']) SETTINGS use_skip_indexes = 0;

SELECT count(), sum(id) FROM tab_string WHERE hasPhrase(v, 'w5 r5005 w5') OR hasPhrase(v, 'r5005 r5005');
SELECT count(), sum(id) FROM tab_string WHERE hasPhrase(v, 'w5 r5005 w5') OR hasPhrase(v, 'r5005 r5005') SETTINGS use_skip_indexes = 0;

SELECT '-- Map columns';

CREATE TABLE tab_map
(
    id UInt64,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs')
)
ENGINE = MergeTree ORDER BY id;

-- The tokens start with the keys, so many of them share long prefixes.
INSERT INTO tab_map SELECT
    number,
    map(
        'attribute.with.a.long.common.prefix.a', toString(cityHash64(number) % 100000),
        'attribute.with.a.long.common.prefix.b', toString(number % 100),
        'id', toString(number))
FROM numbers(100000);

SELECT countIf(tokens != arraySort(tokens))
FROM (SELECT part_name, groupArray(token) AS tokens FROM mergeTreeTextIndex(currentDatabase(), tab_map, idx) GROUP BY part_name)
SETTINGS max_threads = 1;

WITH
    index_rows AS (SELECT token_key, token_value, sum(cardinality) AS num_rows FROM mergeTreeTextIndex(currentDatabase(), tab_map, idx) GROUP BY token_key, token_value),
    data_rows AS (SELECT key, value, count() AS num_rows FROM tab_map ARRAY JOIN mapKeys(m) AS key, mapValues(m) AS value GROUP BY key, value)
SELECT count() FROM ((SELECT * FROM index_rows EXCEPT SELECT * FROM data_rows) UNION ALL (SELECT * FROM data_rows EXCEPT SELECT * FROM index_rows));

SELECT count(), sum(id) FROM tab_map
WHERE m['attribute.with.a.long.common.prefix.a'] = '12345' OR m['attribute.with.a.long.common.prefix.b'] = '42' OR m['id'] = '777';

SELECT count(), sum(id) FROM tab_map
WHERE m['attribute.with.a.long.common.prefix.a'] = '12345' OR m['attribute.with.a.long.common.prefix.b'] = '42' OR m['id'] = '777'
SETTINGS use_skip_indexes = 0;

DROP TABLE tab_array;
DROP TABLE tab_string;
DROP TABLE tab_map;
