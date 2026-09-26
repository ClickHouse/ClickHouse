-- Tags: no-fasttest

-- A text index over a LowCardinality column must hold exactly the tokens and posting lists of the same index over
-- a plain column with the same values, whichever way the index is built. Each case compares the two indexes token by
-- token, and checks the results of searches through the index against a full scan.

SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0;

CREATE TABLE t_main
(
    id UInt64,
    lc50 LowCardinality(String), s50 String,
    lc300 LowCardinality(String), s300 String,
    INDEX i_lc50 lc50 TYPE text(tokenizer = splitByNonAlpha),
    INDEX i_s50 s50 TYPE text(tokenizer = splitByNonAlpha),
    INDEX i_lc300 lc300 TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1),
    INDEX i_s300 s300 TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = 104857600, allow_experimental_text_index_phrase_search = 1;

-- Values share tokens and repeat a token within a value. More than 255 values need 16-bit dictionary positions.
INSERT INTO t_main SELECT number,
    concat('a', toString(number % 50 % 5), ' b', toString(number % 50)) AS v50, v50,
    concat('w', toString(number % 300 % 7), ' v', toString(number % 300), ' w', toString(number % 300 % 7), ' x', toString(number % 300 % 3)) AS v300, v300
FROM numbers(3000);

WITH (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_main, i_lc50)) AS lc,
     (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_main, i_s50)) AS s
SELECT '50 values', lc.1, lc = s;
WITH (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_main, i_lc300)) AS lc,
     (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_main, i_s300)) AS s
SELECT '300 values', lc.1, lc = s;

SELECT 'a3', count(), sum(id) FROM t_main WHERE hasToken(lc50, 'a3');
SELECT 'a3', count(), sum(id) FROM t_main WHERE hasToken(s50, 'a3') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT 'b49', count(), sum(id) FROM t_main WHERE hasToken(lc50, 'b49');
SELECT 'b49', count(), sum(id) FROM t_main WHERE hasToken(s50, 'b49') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT 'w3', count(), sum(id) FROM t_main WHERE hasToken(lc300, 'w3');
SELECT 'w3', count(), sum(id) FROM t_main WHERE hasToken(s300, 'w3') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT 'v299', count(), sum(id) FROM t_main WHERE hasToken(lc300, 'v299');
SELECT 'v299', count(), sum(id) FROM t_main WHERE hasToken(s300, 'v299') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT 'v300', count(), sum(id) FROM t_main WHERE hasToken(lc300, 'v300');
SELECT 'v300', count(), sum(id) FROM t_main WHERE hasToken(s300, 'v300') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT 'v17 w3', count(), sum(id) FROM t_main WHERE hasPhrase(lc300, 'v17 w3');
SELECT 'v17 w3', count(), sum(id) FROM t_main WHERE hasPhrase(s300, 'v17 w3') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT 'w3 w3', count(), sum(id) FROM t_main WHERE hasPhrase(lc300, 'w3 w3');
SELECT 'w3 w3', count(), sum(id) FROM t_main WHERE hasPhrase(s300, 'w3 w3') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

-- NULL, an empty value and a value without tokens; FixedString values with the `array` tokenizer.
CREATE TABLE t_null
(
    id UInt64,
    lcn LowCardinality(Nullable(String)), sn Nullable(String),
    lcf LowCardinality(Nullable(FixedString(3))), sf Nullable(FixedString(3)),
    INDEX i_lcn lcn TYPE text(tokenizer = splitByNonAlpha),
    INDEX i_sn sn TYPE text(tokenizer = splitByNonAlpha),
    INDEX i_lcf lcf TYPE text(tokenizer = array),
    INDEX i_sf sf TYPE text(tokenizer = array)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = 104857600;

INSERT INTO t_null SELECT number,
    multiIf(number % 5 = 0, NULL, number % 7 = 0, '', number % 11 = 0, '...', concat('n', toString(number % 20), ' m')) AS vn, vn,
    if(number % 3 = 0, NULL, ['abc', 'de'][number % 2 + 1]) AS vf, vf
FROM numbers(1000);

WITH (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_null, i_lcn)) AS lc,
     (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_null, i_sn)) AS s
SELECT 'Nullable', lc.1, lc = s;
WITH (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_null, i_lcf)) AS lc,
     (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_null, i_sf)) AS s
SELECT 'Nullable(FixedString)', lc.1, lc = s;

SELECT 'm', count(), sum(id) FROM t_null WHERE hasToken(lcn, 'm');
SELECT 'm', count(), sum(id) FROM t_null WHERE hasToken(sn, 'm') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

-- Arrays with NULL and empty elements, and tokens repeated in a row.
CREATE TABLE t_arr
(
    id UInt64,
    a Array(LowCardinality(Nullable(String))), sa Array(Nullable(String)),
    INDEX i_a a TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1),
    INDEX i_sa sa TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = 104857600, allow_experimental_text_index_phrase_search = 1;

INSERT INTO t_arr SELECT number,
    arrayMap(k -> multiIf(k = 0, NULL, k = 1, '', concat('e', toString((number + k) % 13), ' f e', toString(k))), range(number % 5)) AS v, v
FROM numbers(1000);

WITH (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_arr, i_a)) AS lc,
     (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_arr, i_sa)) AS s
SELECT 'Array', lc.1, lc = s;

SELECT 'e3', count(), sum(id) FROM t_arr WHERE hasAnyTokens(a, 'e3');
SELECT 'e3', count(), sum(id) FROM t_arr WHERE hasAnyTokens(sa, 'e3') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

-- Postprocessors: IN and NOT IN filters, and a postprocessor with phrase search.
CREATE TABLE t_postprocessor
(
    id UInt64,
    lc1 LowCardinality(String), s1 String,
    lc2 LowCardinality(String), s2 String,
    lc3 LowCardinality(String), s3 String,
    INDEX i_lc1 lc1 TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(lc1 IN ('w1', 'w2'), '', lc1)),
    INDEX i_s1 s1 TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(s1 IN ('w1', 'w2'), '', s1)),
    INDEX i_lc2 lc2 TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(lc2 NOT IN ('w1', 'w2'), '', lc2)),
    INDEX i_s2 s2 TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(s2 NOT IN ('w1', 'w2'), '', s2)),
    INDEX i_lc3 lc3 TYPE text(tokenizer = splitByNonAlpha, postprocessor = upper(lc3), support_phrase_search = 1),
    INDEX i_s3 s3 TYPE text(tokenizer = splitByNonAlpha, postprocessor = upper(s3), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = 104857600, allow_experimental_text_index_phrase_search = 1;

INSERT INTO t_postprocessor SELECT number, v, v, v, v, v, v
FROM (SELECT number, concat('w', toString(number % 5), ' z', toString(number % 20)) AS v FROM numbers(1000));

WITH (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_postprocessor, i_lc1)) AS lc,
     (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_postprocessor, i_s1)) AS s
SELECT 'IN', lc.1, lc = s;
WITH (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_postprocessor, i_lc2)) AS lc,
     (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_postprocessor, i_s2)) AS s
SELECT 'NOT IN', lc.1, lc = s;
WITH (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_postprocessor, i_lc3)) AS lc,
     (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_postprocessor, i_s3)) AS s
SELECT 'upper', lc.1, lc = s;

-- Map indexes.
CREATE TABLE t_map
(
    id UInt64,
    m Map(LowCardinality(String), String), ms Map(String, String),
    INDEX i_kv_lc m TYPE text(tokenizer = keyValuePairs),
    INDEX i_kv_s ms TYPE text(tokenizer = keyValuePairs),
    INDEX i_keys_lc mapKeys(m) TYPE text(tokenizer = array),
    INDEX i_keys_s mapKeys(ms) TYPE text(tokenizer = array)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = 104857600;

INSERT INTO t_map SELECT number, map(concat('k', toString(number % 4)), toString(number % 3), 'key', 'value') AS v, v FROM numbers(1000);

WITH (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_map, i_kv_lc)) AS lc,
     (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_map, i_kv_s)) AS s
SELECT 'keyValuePairs', lc.1, lc = s;
WITH (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_map, i_keys_lc)) AS lc,
     (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_map, i_keys_s)) AS s
SELECT 'mapKeys', lc.1, lc = s;

-- The ngrams tokenizer, and a preprocessor that maps several values to one.
CREATE TABLE t_misc
(
    id UInt64,
    lc1 LowCardinality(String), s1 String,
    lc2 LowCardinality(String), s2 String,
    INDEX i_lc1 lc1 TYPE text(tokenizer = ngrams(3)),
    INDEX i_s1 s1 TYPE text(tokenizer = ngrams(3)),
    INDEX i_lc2 lc2 TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(lc2)),
    INDEX i_s2 s2 TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s2))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = 104857600;

INSERT INTO t_misc SELECT number, v1, v1, v2, v2
FROM (SELECT number, concat('abcd', toString(number % 30)) AS v1, ['Foo Bar', 'foo bar', 'FOO BAZ', 'qux'][number % 4 + 1] AS v2 FROM numbers(1000));

WITH (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_misc, i_lc1)) AS lc,
     (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_misc, i_s1)) AS s
SELECT 'ngrams', lc.1, lc = s;
WITH (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_misc, i_lc2)) AS lc,
     (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_misc, i_s2)) AS s
SELECT 'preprocessor', lc.1, lc = s;

-- 9 values and the empty default value: 80 rows reuse the tokens of a value, 79 rows tokenize every row.
CREATE TABLE t_threshold
(
    id UInt64,
    lc LowCardinality(String), s String,
    INDEX i_lc lc TYPE text(tokenizer = splitByNonAlpha),
    INDEX i_s s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = 104857600;

SYSTEM STOP MERGES t_threshold;
INSERT INTO t_threshold SELECT number, concat('c', toString(number % 9)) AS v, v FROM numbers(80);
INSERT INTO t_threshold SELECT 1000 + number, concat('c', toString(number % 9)) AS v, v FROM numbers(79);

WITH (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_threshold, i_lc)) AS lc,
     (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), t_threshold, i_s)) AS s
SELECT 'threshold', lc.1, lc = s;

-- The index built by INSERT, by a merge that flushes several segments, and by MATERIALIZE INDEX.
CREATE TABLE t_insert (id UInt64, lc LowCardinality(String), INDEX i lc TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1000, index_granularity_bytes = 104857600;
CREATE TABLE t_merge (id UInt64, lc LowCardinality(String))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1000, index_granularity_bytes = 104857600, text_index_max_processed_tokens_before_flush = 1000;
CREATE TABLE t_merge_s (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1000, index_granularity_bytes = 104857600, text_index_max_processed_tokens_before_flush = 1000;
CREATE TABLE t_materialize (id UInt64, lc LowCardinality(String))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1000, index_granularity_bytes = 104857600;

SYSTEM STOP MERGES t_merge;
SYSTEM STOP MERGES t_merge_s;
INSERT INTO t_insert SELECT number, concat('a', toString(number % 50 % 5), ' b', toString(number % 50)) FROM numbers(3000);
INSERT INTO t_merge SELECT number, concat('a', toString(number % 50 % 5), ' b', toString(number % 50)) FROM numbers(1500);
INSERT INTO t_merge SELECT number, concat('a', toString(number % 50 % 5), ' b', toString(number % 50)) FROM numbers(1500, 1500);
INSERT INTO t_merge_s SELECT number, concat('a', toString(number % 50 % 5), ' b', toString(number % 50)) FROM numbers(1500);
INSERT INTO t_merge_s SELECT number, concat('a', toString(number % 50 % 5), ' b', toString(number % 50)) FROM numbers(1500, 1500);
INSERT INTO t_materialize SELECT number, concat('a', toString(number % 50 % 5), ' b', toString(number % 50)) FROM numbers(3000);

ALTER TABLE t_merge ADD INDEX i lc TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE t_merge_s ADD INDEX i s TYPE text(tokenizer = splitByNonAlpha);
SYSTEM START MERGES t_merge;
SYSTEM START MERGES t_merge_s;
OPTIMIZE TABLE t_merge FINAL;
OPTIMIZE TABLE t_merge_s FINAL;
ALTER TABLE t_materialize ADD INDEX i lc TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE t_materialize MATERIALIZE INDEX i SETTINGS mutations_sync = 2;

WITH (SELECT (count(), sum(cityHash64(* EXCEPT part_name))) FROM mergeTreeTextIndex(currentDatabase(), t_merge_s, i)) AS s
SELECT 'build',
    (SELECT (count(), sum(cityHash64(* EXCEPT part_name))) FROM mergeTreeTextIndex(currentDatabase(), t_insert, i)) = s,
    (SELECT (count(), sum(cityHash64(* EXCEPT part_name))) FROM mergeTreeTextIndex(currentDatabase(), t_merge, i)) = s,
    (SELECT (count(), sum(cityHash64(* EXCEPT part_name))) FROM mergeTreeTextIndex(currentDatabase(), t_materialize, i)) = s;

SYSTEM FLUSH LOGS part_log;
WITH (SELECT sum(ProfileEvents['TextIndexTemporarySegmentsWritten']) FROM system.part_log
      WHERE database = currentDatabase() AND table = 't_merge' AND event_type = 'MergeParts') AS lc,
     (SELECT sum(ProfileEvents['TextIndexTemporarySegmentsWritten']) FROM system.part_log
      WHERE database = currentDatabase() AND table = 't_merge_s' AND event_type = 'MergeParts') AS s
SELECT 'segments', lc = s, lc > 1;

DROP TABLE t_main;
DROP TABLE t_null;
DROP TABLE t_arr;
DROP TABLE t_postprocessor;
DROP TABLE t_map;
DROP TABLE t_misc;
DROP TABLE t_threshold;
DROP TABLE t_insert;
DROP TABLE t_merge;
DROP TABLE t_merge_s;
DROP TABLE t_materialize;
