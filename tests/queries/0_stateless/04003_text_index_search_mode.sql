-- Tags: no-parallel-replicas

SET optimize_or_like_chain = 0;
SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_search_mode;

CREATE TABLE t_search_mode (a UInt64, s String, index idx_s (s) type text(tokenizer = ngrams(2))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_search_mode SELECT number, toString(number) FROM numbers(100000);

SELECT count() FROM t_search_mode WHERE s LIKE '%123%';

SELECT count() FROM t_search_mode WHERE s LIKE '%123%' OR s LIKE '%234%';

SELECT count() FROM t_search_mode WHERE s LIKE '%123%' AND s LIKE '%234%';

SELECT count() FROM t_search_mode WHERE s LIKE '%123%' AND a > 10000;

DROP TABLE t_search_mode;
