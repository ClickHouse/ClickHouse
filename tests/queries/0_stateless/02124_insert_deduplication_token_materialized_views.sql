-- Tags: long

select 'deduplicate_blocks_in_dependent_materialized_views=0, insert_deduplication_token = no, results: test_mv_a and test_mv_c have all data, test_mv_b has data obly with max_partitions_per_insert_block=0';

drop table if exists test  sync;
drop table if exists test_mv_a sync;
drop table if exists test_mv_b sync;
drop table if exists test_mv_c sync;

set deduplicate_blocks_in_dependent_materialized_views=0;

CREATE TABLE test (test String, A Int64, B Int64) ENGINE = ReplicatedMergeTree ('/clickhouse/tables/{database}/test_02124/{table}', '1')
ORDER BY tuple();

CREATE MATERIALIZED VIEW test_mv_a Engine=ReplicatedMergeTree ('/clickhouse/tables/{database}/test_02124/{table}', '1')
order by tuple() AS SELECT test, A, count() c FROM test group by test, A;

CREATE MATERIALIZED VIEW test_mv_b Engine=ReplicatedMergeTree ('/clickhouse/tables/{database}/test_02124/{table}', '1')
partition by A order by tuple() AS SELECT test, A, count() c FROM test group by test, A;

CREATE MATERIALIZED VIEW test_mv_c Engine=ReplicatedMergeTree ('/clickhouse/tables/{database}/test_02124/{table}', '1')
order by tuple() AS SELECT test, A, count() c FROM test group by test, A;

SET max_partitions_per_insert_block = 1;
INSERT INTO test SELECT 'case1', number%3, 1 FROM numbers(9); -- { serverError TOO_MANY_PARTS }
SET max_partitions_per_insert_block = 0;
INSERT INTO test SELECT 'case1', number%3, 1 FROM numbers(9);
INSERT INTO test SELECT 'case1', number%3, 2 FROM numbers(9);
INSERT INTO test SELECT 'case1', number%3, 2 FROM numbers(9);

select
  (select count() from test where test='case1'),
  (select sum(c) from test_mv_a where test='case1'),
  (select sum(c) from test_mv_b where test='case1'),
  (select sum(c) from test_mv_c where test='case1');


select 'deduplicate_blocks_in_dependent_materialized_views=1, insert_deduplication_token = no, results: all tables have deduplicated data';

set deduplicate_blocks_in_dependent_materialized_views=1;

SET max_partitions_per_insert_block = 1;
INSERT INTO test SELECT 'case2', number%3, 1 FROM numbers(9) ; -- { serverError TOO_MANY_PARTS }
SET max_partitions_per_insert_block = 0;
INSERT INTO test SELECT 'case2', number%3, 1 FROM numbers(9);
INSERT INTO test SELECT 'case2', number%3, 2 FROM numbers(9);
INSERT INTO test SELECT 'case2', number%3, 2 FROM numbers(9);

select
  (select count() from test where test='case2'),
  (select sum(c) from test_mv_a where test='case2'),
  (select sum(c) from test_mv_b where test='case2'),
  (select sum(c) from test_mv_c where test='case2');


select 'deduplicate_blocks_in_dependent_materialized_views=0, insert_deduplication_token = yes, results: test_mv_a and test_mv_c have all data, test_mv_b has data obly with max_partitions_per_insert_block=0';

set deduplicate_blocks_in_dependent_materialized_views=0;

SET max_partitions_per_insert_block = 1;
INSERT INTO test SELECT 'case3', number%3, 1 FROM numbers(9) SETTINGS insert_deduplication_token = 'case3test1'; -- { serverError TOO_MANY_PARTS }
SET max_partitions_per_insert_block = 0;
INSERT INTO test SELECT 'case3', number%3, 1 FROM numbers(9) SETTINGS insert_deduplication_token = 'case3test1';
INSERT INTO test SELECT 'case3', number%3, 2 FROM numbers(9) SETTINGS insert_deduplication_token = 'case3test2';
INSERT INTO test SELECT 'case3', number%3, 2 FROM numbers(9) SETTINGS insert_deduplication_token = 'case3test2';

select
  (select count() from test where test='case3'),
  (select sum(c) from test_mv_a where test='case3'),
  (select sum(c) from test_mv_b where test='case3'),
  (select sum(c) from test_mv_c where test='case3');

select 'deduplicate_blocks_in_dependent_materialized_views=1, insert_deduplication_token = yes, results: all tables have deduplicated data';

set deduplicate_blocks_in_dependent_materialized_views=1;

SET max_partitions_per_insert_block = 1;
INSERT INTO test SELECT 'case4', number%3, 1 FROM numbers(9) SETTINGS insert_deduplication_token = 'case4test1' ; -- { serverError TOO_MANY_PARTS }
SET max_partitions_per_insert_block = 0;
INSERT INTO test SELECT 'case4', number%3, 1 FROM numbers(9) SETTINGS insert_deduplication_token = 'case4test1';
INSERT INTO test SELECT 'case4', number%3, 2 FROM numbers(9) SETTINGS insert_deduplication_token = 'case4test2';
INSERT INTO test SELECT 'case4', number%3, 2 FROM numbers(9) SETTINGS insert_deduplication_token = 'case4test2';

select
  (select count() from test where test='case4'),
  (select sum(c) from test_mv_a where test='case4'),
  (select sum(c) from test_mv_b where test='case4'),
  (select sum(c) from test_mv_c where test='case4');

drop table test sync;
drop table test_mv_a sync;
drop table test_mv_b sync;
drop table test_mv_c sync;
