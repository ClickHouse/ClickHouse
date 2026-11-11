-- A store_source NAIVE_BAYES dictionary must load when a source column is read in sparse serialization: the
-- retained source columns are materialized to full before accumulation. The source below makes class_id sparse
-- (a dense part followed by an all-default part); the single-threaded small-block read yields a dense block
-- before a sparse one, which is the case that must be handled.

DROP DICTIONARY IF EXISTS nb_sparse_src_dict;
DROP TABLE IF EXISTS nb_sparse_src;

CREATE TABLE nb_sparse_src (ngram String, class_id UInt32, count UInt64)
ENGINE = MergeTree ORDER BY ngram
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, min_bytes_for_wide_part = 0;
INSERT INTO nb_sparse_src SELECT 'a' || toString(number), 7, 1 FROM numbers(20000); -- non-default class_id -> dense part
INSERT INTO nb_sparse_src SELECT 'b' || toString(number), 0, 1 FROM numbers(20000); -- all-default class_id -> sparse part

CREATE DICTIONARY nb_sparse_src_dict (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_sparse_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' store_source 1))
LIFETIME(0)
SETTINGS(max_threads = 1, max_block_size = 4096);

-- store_source retains every source row, including the sparse part, with correct class ids.
SELECT count(), countIf(class_id = 7), countIf(class_id = 0) FROM nb_sparse_src_dict;
-- Classification works on a token from the dense class.
SELECT naiveBayesClassifier('nb_sparse_src_dict', 'a1');

DROP DICTIONARY nb_sparse_src_dict;
DROP TABLE nb_sparse_src;
