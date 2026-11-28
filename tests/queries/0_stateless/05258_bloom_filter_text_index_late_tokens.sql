-- A word that first appears in the last rows of a large granule must still be found through tokenbf_v1,
-- ngrambf_v1 and sparse_grams indexes, whether the rows before it repeat the same words or rarely repeat.
-- The INSERTs must also report the repeated words in the BloomFilterTextIndexRepeatedTokens profile event,
-- and an index on (r, s) must report at least as many as an index on s alone, even when the words of r rarely repeat.
-- Random settings limits: max_insert_threads=(1, 1); use_skip_indexes_on_data_read=(0, 0); use_query_condition_cache=(0, 0)
-- Tags: no-parallel-replicas

DROP TABLE IF EXISTS t_token;
DROP TABLE IF EXISTS t_ngram;
DROP TABLE IF EXISTS t_sparse;
DROP TABLE IF EXISTS t_two;
DROP TABLE IF EXISTS t_one;

CREATE TABLE t_token (id UInt64, s String, INDEX idx s TYPE tokenbf_v1(65536, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
-- Rows 0..8191 repeat the same words; row 8000 also holds two words of its own.
INSERT INTO t_token SELECT number, concat('GET /api/v1/items status=200 ok', if(number = 8000, ' wqxjvz qwpzkxvjbmtr', '')) FROM numbers(8192);
-- Rows 8192..16383 rarely repeat a word; row 16300 also holds two words of its own and the path of the first rows.
INSERT INTO t_token SELECT number, concat(toString(cityHash64(number) % 1000000), ' ', toString(cityHash64(number, 1) % 1000000), if(number = 16300, ' zvkqpm pmzvtqkxwrjb /api/v1/items', '')) FROM numbers(8192, 8192);

SELECT count() FROM t_token WHERE hasToken(s, 'wqxjvz') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_token WHERE hasToken(s, 'zvkqpm') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_token WHERE hasToken(s, 'items') SETTINGS force_data_skipping_indices = 'idx';

OPTIMIZE TABLE t_token FINAL;

SELECT count() FROM t_token WHERE hasToken(s, 'wqxjvz') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_token WHERE hasToken(s, 'zvkqpm') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_token WHERE hasToken(s, 'items') SETTINGS force_data_skipping_indices = 'idx';

SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_token WHERE hasToken(s, 'wqxjvz')) WHERE explain ILIKE '%Granules: 1/2%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_token WHERE hasToken(s, 'zvkqpm')) WHERE explain ILIKE '%Granules: 1/2%';

CREATE TABLE t_ngram (id UInt64, s String, INDEX idx s TYPE ngrambf_v1(3, 65536, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
-- Rows 0..8191 repeat the same words; row 8000 also holds two words of its own.
INSERT INTO t_ngram SELECT number, concat('GET /api/v1/items status=200 ok', if(number = 8000, ' wqxjvz qwpzkxvjbmtr', '')) FROM numbers(8192);
-- Rows 8192..16383 rarely repeat a word; row 16300 also holds two words of its own and the path of the first rows.
INSERT INTO t_ngram SELECT number, concat(toString(cityHash64(number) % 1000000), ' ', toString(cityHash64(number, 1) % 1000000), if(number = 16300, ' zvkqpm pmzvtqkxwrjb /api/v1/items', '')) FROM numbers(8192, 8192);

SELECT count() FROM t_ngram WHERE s LIKE '%wqxjvz%' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_ngram WHERE s LIKE '%zvkqpm%' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_ngram WHERE s LIKE '%/api/v1/items%' SETTINGS force_data_skipping_indices = 'idx';

OPTIMIZE TABLE t_ngram FINAL;

SELECT count() FROM t_ngram WHERE s LIKE '%wqxjvz%' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_ngram WHERE s LIKE '%zvkqpm%' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_ngram WHERE s LIKE '%/api/v1/items%' SETTINGS force_data_skipping_indices = 'idx';

SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram WHERE s LIKE '%wqxjvz%') WHERE explain ILIKE '%Granules: 1/2%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram WHERE s LIKE '%zvkqpm%') WHERE explain ILIKE '%Granules: 1/2%';

CREATE TABLE t_sparse (id UInt64, s String, INDEX idx s TYPE sparse_grams(3, 8, 65536, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
-- Rows 0..8191 repeat the same words; row 8000 also holds two words of its own.
INSERT INTO t_sparse SELECT number, concat('GET /api/v1/items status=200 ok', if(number = 8000, ' wqxjvz qwpzkxvjbmtr', '')) FROM numbers(8192);
-- Rows 8192..16383 rarely repeat a word; row 16300 also holds two words of its own and the path of the first rows.
INSERT INTO t_sparse SELECT number, concat(toString(cityHash64(number) % 1000000), ' ', toString(cityHash64(number, 1) % 1000000), if(number = 16300, ' zvkqpm pmzvtqkxwrjb /api/v1/items', '')) FROM numbers(8192, 8192);

SELECT count() FROM t_sparse WHERE s LIKE '%qwpzkxvjbmtr%' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_sparse WHERE s LIKE '%pmzvtqkxwrjb%' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_sparse WHERE s LIKE '%/api/v1/items%' SETTINGS force_data_skipping_indices = 'idx';

OPTIMIZE TABLE t_sparse FINAL;

SELECT count() FROM t_sparse WHERE s LIKE '%qwpzkxvjbmtr%' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_sparse WHERE s LIKE '%pmzvtqkxwrjb%' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_sparse WHERE s LIKE '%/api/v1/items%' SETTINGS force_data_skipping_indices = 'idx';

SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sparse WHERE s LIKE '%qwpzkxvjbmtr%') WHERE explain ILIKE '%Granules: 1/2%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sparse WHERE s LIKE '%pmzvtqkxwrjb%') WHERE explain ILIKE '%Granules: 1/2%';

-- s repeats the same words in every row, r rarely repeats a word.
CREATE TABLE t_two (id UInt64, r String, s String, INDEX idx (r, s) TYPE tokenbf_v1(65536, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_one (id UInt64, r String, s String, INDEX idx s TYPE tokenbf_v1(65536, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_two SELECT number, concat(toString(cityHash64(number) % 1000000), ' ', toString(cityHash64(number, 1) % 1000000)), 'GET /api/v1/items status=200 ok' FROM numbers(8192);
INSERT INTO t_one SELECT number, concat(toString(cityHash64(number) % 1000000), ' ', toString(cityHash64(number, 1) % 1000000)), 'GET /api/v1/items status=200 ok' FROM numbers(8192);

SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['BloomFilterTextIndexRepeatedTokens']) > 0 FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND query_kind = 'Insert' AND has(tables, currentDatabase() || '.t_token');
SELECT sum(ProfileEvents['BloomFilterTextIndexRepeatedTokens']) > 0 FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND query_kind = 'Insert' AND has(tables, currentDatabase() || '.t_ngram');
SELECT sum(ProfileEvents['BloomFilterTextIndexRepeatedTokens']) > 0 FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND query_kind = 'Insert' AND has(tables, currentDatabase() || '.t_sparse');
SELECT sumIf(ProfileEvents['BloomFilterTextIndexRepeatedTokens'], has(tables, currentDatabase() || '.t_two'))
        >= sumIf(ProfileEvents['BloomFilterTextIndexRepeatedTokens'], has(tables, currentDatabase() || '.t_one'))
    AND sumIf(ProfileEvents['BloomFilterTextIndexRepeatedTokens'], has(tables, currentDatabase() || '.t_one')) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND query_kind = 'Insert';

DROP TABLE t_token;
DROP TABLE t_ngram;
DROP TABLE t_sparse;
DROP TABLE t_two;
DROP TABLE t_one;
