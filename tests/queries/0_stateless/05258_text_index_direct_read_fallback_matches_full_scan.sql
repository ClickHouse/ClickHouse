-- Tags: no-parallel-replicas

-- When a text index cannot answer a LIKE pattern or a phrase from its posting lists, a direct read
-- evaluates the predicate on the column data instead. Such reads must return exactly the rows a
-- table without the index returns, for every granule of the part and every read block shape.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET max_insert_threads = 1;

-- Make every pattern below give up its dictionary scan, and every phrase below count as not
-- selective enough, on every execution.
SET text_index_like_max_postings_to_read = 0;
SET text_index_hint_max_selectivity = 0;
SET use_text_index_tokens_cache = 0;
SET use_text_index_negative_tokens_cache = 0;
SET use_text_index_header_cache = 0;
SET use_text_index_postings_cache = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_nullable;
DROP TABLE IF EXISTS t_low_cardinality;
DROP TABLE IF EXISTS t_phrase;

-- 303 rows in granules of 8 rows: many granules per read block, and a last granule of 7 rows.
-- The only rows containing 'lastgranule' are the last 7.
CREATE TABLE t_plain (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_plain SELECT number, concat(
    'forum', toString(number % 40), ' alpha', toString(number % 7),
    if(number >= 296, ' lastgranule', ''),
    multiIf(number % 5 = 0, ' see the kitten', number % 5 = 1, ' the see kitten', number % 5 = 2, ' see kitten', number % 5 = 3, ' kitten see', ' see a kitten'))
FROM numbers(303);

CREATE TABLE t_wide (id UInt32, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_wide SELECT * FROM t_plain;

CREATE TABLE t_compact (id UInt32, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;
INSERT INTO t_compact SELECT * FROM t_plain;

CREATE TABLE t_nullable (id UInt32, s Nullable(String), INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_nullable SELECT * FROM t_plain;

CREATE TABLE t_low_cardinality (id UInt32, s LowCardinality(String), INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_low_cardinality SELECT * FROM t_plain;

CREATE TABLE t_phrase (id UInt32, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1))
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1, index_granularity = 8, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_phrase SELECT * FROM t_plain;

SELECT 'part types', groupArray(part_type) FROM (SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table IN ('t_wide', 't_compact') AND active ORDER BY table DESC);

-- Each line: whether the rows equal those of the table without the index, and how many rows there are.
SELECT 'infix', arraySort(groupArray(id)) = (SELECT arraySort(groupArray(id)) FROM t_plain WHERE s LIKE '%alpha3%'), count() FROM t_wide WHERE s LIKE '%alpha3%';
SELECT 'last granule only', arraySort(groupArray(id)) = (SELECT arraySort(groupArray(id)) FROM t_plain WHERE s LIKE '%lastgranule%'), count() FROM t_wide WHERE s LIKE '%lastgranule%';
SELECT 'every row', arraySort(groupArray(id)) = (SELECT arraySort(groupArray(id)) FROM t_plain WHERE s LIKE '%forum%'), count() FROM t_wide WHERE s LIKE '%forum%';
SELECT 'no row', count() FROM t_wide WHERE s LIKE '%alpha3%' AND s LIKE '%alpha4%';
SELECT 'ilike', arraySort(groupArray(id)) = (SELECT arraySort(groupArray(id)) FROM t_plain WHERE s ILIKE '%ALPHA3%'), count() FROM t_wide WHERE s ILIKE '%ALPHA3%';
SELECT 'two granules per block, prewhere', arraySort(groupArray(id)) = (SELECT arraySort(groupArray(id)) FROM t_plain PREWHERE id % 3 != 0 WHERE s LIKE '%alpha3%'), count() FROM t_wide PREWHERE id % 3 != 0 WHERE s LIKE '%alpha3%' SETTINGS max_block_size = 16;
SELECT 'four threads', arraySort(groupArray(id)) = (SELECT arraySort(groupArray(id)) FROM t_plain WHERE s LIKE '%alpha3%'), count() FROM t_wide WHERE s LIKE '%alpha3%' SETTINGS max_threads = 4, max_block_size = 16;
SELECT 'compact part', arraySort(groupArray(id)) = (SELECT arraySort(groupArray(id)) FROM t_plain WHERE s LIKE '%lastgranule%' OR s LIKE '%alpha3%'), count() FROM t_compact WHERE s LIKE '%lastgranule%' OR s LIKE '%alpha3%';
SELECT 'nullable', arraySort(groupArray(id)) = (SELECT arraySort(groupArray(id)) FROM t_plain WHERE s LIKE '%lastgranule%' OR s LIKE '%alpha3%'), count() FROM t_nullable WHERE s LIKE '%lastgranule%' OR s LIKE '%alpha3%';
SELECT 'low cardinality', arraySort(groupArray(id)) = (SELECT arraySort(groupArray(id)) FROM t_plain WHERE s LIKE '%lastgranule%' OR s LIKE '%alpha3%'), count() FROM t_low_cardinality WHERE s LIKE '%lastgranule%' OR s LIKE '%alpha3%';

-- 'see kitten' is adjacent only in 'the see kitten' and 'see kitten'; 'see the kitten', 'see a kitten' and 'kitten see' must not match.
SELECT 'phrase', arraySort(groupArray(id)) = (SELECT arraySort(groupArray(id)) FROM t_plain WHERE hasPhrase(s, 'see kitten')), count() FROM t_phrase WHERE hasPhrase(s, 'see kitten');
SELECT 'phrase, last granule', arraySort(groupArray(id)) = (SELECT arraySort(groupArray(id)) FROM t_plain WHERE hasPhrase(s, 'lastgranule see')), count() FROM t_phrase WHERE hasPhrase(s, 'lastgranule see');

SYSTEM FLUSH LOGS query_log;

-- Every query above took the path under test.
SELECT 'every pattern query fell back', count() > 0, min(ProfileEvents['TextIndexDiscardPatternScan'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND query LIKE 'SELECT \'%' AND query LIKE '%LIKE \'%' AND query NOT LIKE '%system.%';
SELECT 'every phrase query fell back', count() > 0, min(ProfileEvents['TextIndexPhraseFallbacks'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND query LIKE 'SELECT \'phrase%';

DROP TABLE t_plain;
DROP TABLE t_wide;
DROP TABLE t_compact;
DROP TABLE t_nullable;
DROP TABLE t_low_cardinality;
DROP TABLE t_phrase;
