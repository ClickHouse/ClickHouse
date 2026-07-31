-- Tags: no-parallel-replicas

SET optimize_or_like_chain = 0;

DROP TABLE IF EXISTS t_search_mode;

CREATE TABLE t_search_mode (a UInt64, s String, index idx_s (s) type text(tokenizer = ngrams(2))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_search_mode SELECT number, toString(number) FROM numbers(100000);

SELECT count() FROM t_search_mode WHERE s LIKE '%123%';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_search_mode WHERE s LIKE '%123%') WHERE explain like '%Condition%';

SELECT count() FROM t_search_mode WHERE s LIKE '%123%' OR s LIKE '%234%';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_search_mode WHERE s LIKE '%123%' OR s LIKE '%234%') WHERE explain like '%Condition%';

SELECT count() FROM t_search_mode WHERE s LIKE '%123%' AND s LIKE '%234%';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_search_mode WHERE s LIKE '%123%' AND s LIKE '%234%') WHERE explain like '%Condition%';

SELECT count() FROM t_search_mode WHERE s LIKE '%123%' AND a > 10000;
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_search_mode WHERE s LIKE '%123%' AND a > 10000) WHERE explain like '%Condition%';

DROP TABLE t_search_mode;
