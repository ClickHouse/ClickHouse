SET parallel_replicas_local_plan=1;

drop table if exists foo;

CREATE TABLE foo (id UInt8, path String) engine = MergeTree ORDER BY (path) SETTINGS index_granularity=1;

INSERT INTO foo VALUES (1, 'xxx|yyy'),
(2, 'xxx(zzz'),
(3, 'xxx)zzz'),
(4, 'xxx^zzz'),
(5, 'xxx$zzz'),
(6, 'xxx.zzz'),
(7, 'xxx[zzz'),
(8, 'xxx]zzz'),
(9, 'xxx?zzz'),
(10, 'xxx*zzz'),
(11, 'xxx+zzz'),
(12, 'xxx\\zzz'),
(13, 'xxx{zzz'),
(14, 'xxx}zzz'),
(15, 'xxx-zzz');


-- check if also escaped sequence are properly extracted
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\(zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\(zzz') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\)zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\)zzz') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\^zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\^zzz') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\$zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\$zzz') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\.zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\.zzz') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\[zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\[zzz') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\]zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\]zzz') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\?zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\?zzz') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\*zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\*zzz') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\+zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\+zzz') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\\\zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\\\zzz') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\{zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\{zzz') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\}zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\}zzz') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\-zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\-zzz') SETTINGS force_primary_key = 1;


-- those regex chars prevent the index use (only 3 first chars used during index scan)
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\0bla')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\0bla') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx(bla)')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx(bla)') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx[bla]')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx[bla]') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx^bla')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx^bla') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx.bla')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx.bla') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx+bla')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx+bla') SETTINGS force_primary_key = 1;


-- here the forth char is not used during index, because it has 0+ quantifier
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxxx{0,1}')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxxx{0,1}') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxxx?')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxxx?') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxxx*')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxxx*') SETTINGS force_primary_key = 1;

-- some unsupported regex chars - only 3 first chars used during index scan
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\d+')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\d+') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\w+')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\w+') SETTINGS force_primary_key = 1;


-- fully disabled for pipes - see https://github.com/ClickHouse/ClickHouse/pull/54696
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\|zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\|zzz') SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxxx|foo')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxxx|foo') SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
