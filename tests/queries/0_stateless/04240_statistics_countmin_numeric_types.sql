-- Tags: no-fasttest, no-parallel-replicas
-- no-fasttest: 'countmin' sketches need a 3rd party library

SET allow_statistics = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1;

DROP TABLE IF EXISTS test_f32;
CREATE TABLE test_f32 (a Float32 STATISTICS(countmin), b Float32 STATISTICS(countmin)) ENGINE = MergeTree() ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO test_f32 SELECT 1.5, 1.5 FROM numbers(10);
INSERT INTO test_f32 SELECT 1.5, 99.5 FROM numbers(990);
INSERT INTO test_f32 SELECT 99.5, 99.5 FROM numbers(10);

DROP TABLE IF EXISTS test_f64;
CREATE TABLE test_f64 (a Float64 STATISTICS(countmin), b Float64 STATISTICS(countmin)) ENGINE = MergeTree() ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO test_f64 SELECT 1.5, 1.5 FROM numbers(10);
INSERT INTO test_f64 SELECT 1.5, 99.5 FROM numbers(990);
INSERT INTO test_f64 SELECT 99.5, 99.5 FROM numbers(10);

DROP TABLE IF EXISTS test_i16;
CREATE TABLE test_i16 (a Int16 STATISTICS(countmin), b Int16 STATISTICS(countmin)) ENGINE = MergeTree() ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO test_i16 SELECT 15, 15 FROM numbers(10);
INSERT INTO test_i16 SELECT 15, 995 FROM numbers(990);
INSERT INTO test_i16 SELECT 995, 995 FROM numbers(10);

SELECT 'Float32 - expect b first (BUG: shows a first):';
SELECT replaceRegexpAll(explain, '__table1\\.', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM test_f32 WHERE a = 1.5 AND b = 1.5) WHERE explain LIKE '%Prewhere%';

SELECT 'Float64 - expect b first (CORRECT):';
SELECT replaceRegexpAll(explain, '__table1\\.', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM test_f64 WHERE a = 1.5 AND b = 1.5) WHERE explain LIKE '%Prewhere%';

SELECT 'Int16 - expect b first (CORRECT):';
SELECT replaceRegexpAll(explain, '__table1\\.', '') FROM (EXPLAIN actions=1 SELECT count(*) FROM test_i16 WHERE a = 15 AND b = 15) WHERE explain LIKE '%Prewhere%';

DROP TABLE test_f32;
DROP TABLE test_f64;
DROP TABLE test_i16;
