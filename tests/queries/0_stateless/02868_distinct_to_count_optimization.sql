drop table if exists test_rewrite_uniq_to_count;

CREATE TABLE test_rewrite_uniq_to_count
(
    `a` UInt8,
    `b` UInt8,
    `c` UInt8
) ENGINE = MergeTree ORDER BY `a`;


INSERT INTO test_rewrite_uniq_to_count values ('1', '1', '1'), ('1', '1', '1');
INSERT INTO test_rewrite_uniq_to_count values ('2', '2', '2'), ('2', '2', '2');
INSERT INTO test_rewrite_uniq_to_count values ('3', '3', '3'), ('3', '3', '3');

set optimize_uniq_to_count=true;


SELECT '1. test simple distinct';
SELECT uniq(a) FROM (SELECT DISTINCT a FROM test_rewrite_uniq_to_count) settings enable_analyzer=0;
EXPLAIN SYNTAX SELECT uniq(a) FROM (SELECT DISTINCT a FROM test_rewrite_uniq_to_count) settings enable_analyzer=0;
SELECT uniq(a) FROM (SELECT DISTINCT a FROM test_rewrite_uniq_to_count) settings enable_analyzer=1;
EXPLAIN QUERY TREE SELECT uniq(a) FROM (SELECT DISTINCT a FROM test_rewrite_uniq_to_count) settings enable_analyzer=1;


SELECT '2. test distinct with subquery alias';
SELECT uniq(t.a) FROM (SELECT DISTINCT a FROM test_rewrite_uniq_to_count) t settings enable_analyzer=0;
EXPLAIN SYNTAX SELECT uniq(a) FROM (SELECT DISTINCT a FROM test_rewrite_uniq_to_count) t settings enable_analyzer=0;
SELECT uniq(t.a) FROM (SELECT DISTINCT a FROM test_rewrite_uniq_to_count) t settings enable_analyzer=1;
EXPLAIN QUERY TREE SELECT uniq(t.a) FROM (SELECT DISTINCT a FROM test_rewrite_uniq_to_count) t settings enable_analyzer=1;

SELECT '3. test distinct with compound column name';
SELECT uniq(a) FROM (SELECT DISTINCT test_rewrite_uniq_to_count.a FROM test_rewrite_uniq_to_count) t settings enable_analyzer=0;
EXPLAIN SYNTAX SELECT uniq(a) FROM (SELECT DISTINCT test_rewrite_uniq_to_count.a FROM test_rewrite_uniq_to_count) t settings enable_analyzer=0;
SELECT uniq(a) FROM (SELECT DISTINCT test_rewrite_uniq_to_count.a FROM test_rewrite_uniq_to_count) t settings enable_analyzer=1;
EXPLAIN QUERY TREE SELECT uniq(a) FROM (SELECT DISTINCT test_rewrite_uniq_to_count.a FROM test_rewrite_uniq_to_count) t settings enable_analyzer=1;

SELECT '4. test distinct with select expression alias';
SELECT uniq(alias_of_a) FROM (SELECT DISTINCT a as alias_of_a FROM test_rewrite_uniq_to_count) t settings enable_analyzer=0;
EXPLAIN SYNTAX SELECT uniq(alias_of_a) FROM (SELECT DISTINCT a as alias_of_a FROM test_rewrite_uniq_to_count) t settings enable_analyzer=0;
SELECT uniq(alias_of_a) FROM (SELECT DISTINCT a as alias_of_a FROM test_rewrite_uniq_to_count) t settings enable_analyzer=1;
EXPLAIN QUERY TREE SELECT uniq(alias_of_a) FROM (SELECT DISTINCT a as alias_of_a FROM test_rewrite_uniq_to_count) t settings enable_analyzer=1;


SELECT '5. test simple group by';
SELECT uniq(a) FROM (SELECT a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a) settings enable_analyzer=0;
EXPLAIN SYNTAX SELECT uniq(a) FROM (SELECT a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a) settings enable_analyzer=0;
SELECT uniq(a) FROM (SELECT a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a) settings enable_analyzer=1;
EXPLAIN QUERY TREE SELECT uniq(a) FROM (SELECT a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a) settings enable_analyzer=1;

SELECT '6. test group by with subquery alias';
SELECT uniq(t.a) FROM (SELECT a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a) t settings enable_analyzer=0;
EXPLAIN SYNTAX SELECT uniq(t.a) FROM (SELECT a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a) t settings enable_analyzer=0;
SELECT uniq(t.a) FROM (SELECT a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a) t settings enable_analyzer=1;
EXPLAIN QUERY TREE SELECT uniq(t.a) FROM (SELECT a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a) t settings enable_analyzer=1;

SELECT '7. test group by with compound column name';
SELECT uniq(t.alias_of_a) FROM (SELECT a as alias_of_a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a) t settings enable_analyzer=0;
EXPLAIN SYNTAX SELECT uniq(t.alias_of_a) FROM (SELECT a as alias_of_a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a) t settings enable_analyzer=0;
SELECT uniq(t.alias_of_a) FROM (SELECT a as alias_of_a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a) t settings enable_analyzer=1;
EXPLAIN QUERY TREE SELECT uniq(t.alias_of_a) FROM (SELECT a as alias_of_a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a) t settings enable_analyzer=1;

SELECT '8. test group by with select expression alias';
SELECT uniq(t.alias_of_a) FROM (SELECT a as alias_of_a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY alias_of_a) t settings enable_analyzer=0;
EXPLAIN SYNTAX SELECT uniq(t.alias_of_a) FROM (SELECT a as alias_of_a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY alias_of_a) t  settings enable_analyzer=0;
SELECT uniq(t.alias_of_a) FROM (SELECT a as alias_of_a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY alias_of_a) t  settings enable_analyzer=1;
EXPLAIN QUERY TREE SELECT uniq(t.alias_of_a) FROM (SELECT a as alias_of_a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY alias_of_a) t  settings enable_analyzer=1;

drop table if exists test_rewrite_uniq_to_count;
