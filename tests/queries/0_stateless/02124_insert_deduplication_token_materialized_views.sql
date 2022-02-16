select 'deduplicate_blocks_in_dependent_materialized_views=0, insert_deduplication_token = no, results inconsitent';

drop table if exists test  sync;
drop table if exists test_mv_a sync;
drop table if exists test_mv_b sync;
drop table if exists test_mv_c sync;

set deduplicate_blocks_in_dependent_materialized_views=0;

CREATE TABLE test (A Int64, B Int64) ENGINE = ReplicatedMergeTree ('/clickhouse/test/tables/test','1') ORDER BY tuple()  ;

CREATE MATERIALIZED VIEW test_mv_a Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_a','1') 
order by tuple()  AS SELECT A, count() c FROM test group by A;

CREATE MATERIALIZED VIEW test_mv_b Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_b','1') 
partition by A order by tuple()  AS SELECT A, count() c FROM test group by A;

CREATE MATERIALIZED VIEW test_mv_c Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_c','1') 
order by tuple()  AS SELECT A, count() c FROM test group by A;

SET max_partitions_per_insert_block = 1;
INSERT INTO test SELECT number%3, 1 FROM numbers(9); -- { serverError 252 }
SET max_partitions_per_insert_block = 0;
INSERT INTO test SELECT number%3, 1 FROM numbers(9);
INSERT INTO test SELECT number%3, 2 FROM numbers(9);
INSERT INTO test SELECT number%3, 2 FROM numbers(9);

select
  (select count() from test),
  (select sum(c) from test_mv_a),
  (select sum(c) from test_mv_b),
  (select sum(c) from test_mv_c);


select 'deduplicate_blocks_in_dependent_materialized_views=1, insert_deduplication_token = no, results inconsitent';

drop table if exists test  sync;
drop table if exists test_mv_a sync;
drop table if exists test_mv_b sync;
drop table if exists test_mv_c sync;

set deduplicate_blocks_in_dependent_materialized_views=1;

CREATE TABLE test (A Int64, B Int64) ENGINE = ReplicatedMergeTree ('/clickhouse/test/tables/test','1') ORDER BY tuple()  ;

CREATE MATERIALIZED VIEW test_mv_a Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_a','1') 
order by tuple()  AS SELECT A, count() c FROM test group by A;

CREATE MATERIALIZED VIEW test_mv_b Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_b','1') 
partition by A order by tuple()  AS SELECT A, count() c FROM test group by A;

CREATE MATERIALIZED VIEW test_mv_c Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_c','1') 
order by tuple()  AS SELECT A, count() c FROM test group by A;

SET max_partitions_per_insert_block = 1;
INSERT INTO test SELECT number%3, 1 FROM numbers(9) ; -- { serverError 252 }
SET max_partitions_per_insert_block = 0;
INSERT INTO test SELECT number%3, 1 FROM numbers(9);
INSERT INTO test SELECT number%3, 2 FROM numbers(9);
INSERT INTO test SELECT number%3, 2 FROM numbers(9);

select
  (select count() from test),
  (select sum(c) from test_mv_a),
  (select sum(c) from test_mv_b),
  (select sum(c) from test_mv_c);


select 'deduplicate_blocks_in_dependent_materialized_views=0, insert_deduplication_token = yes, results inconsitent';

drop table if exists test  sync;
drop table if exists test_mv_a sync;
drop table if exists test_mv_b sync;
drop table if exists test_mv_c sync;

set deduplicate_blocks_in_dependent_materialized_views=0;

CREATE TABLE test (A Int64, B Int64) ENGINE = ReplicatedMergeTree ('/clickhouse/test/tables/test','1') ORDER BY tuple()  ;

CREATE MATERIALIZED VIEW test_mv_a Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_a','1') 
order by tuple()  AS SELECT A, count() c FROM test group by A;

CREATE MATERIALIZED VIEW test_mv_b Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_b','1') 
partition by A order by tuple()  AS SELECT A, count() c FROM test group by A;

CREATE MATERIALIZED VIEW test_mv_c Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_c','1') 
order by tuple()  AS SELECT A, count() c FROM test group by A;

SET max_partitions_per_insert_block = 1;
INSERT INTO test SELECT number%3, 1 FROM numbers(9) SETTINGS insert_deduplication_token = 'test1'; -- { serverError 252 }
SET max_partitions_per_insert_block = 0;
INSERT INTO test SELECT number%3, 1 FROM numbers(9) SETTINGS insert_deduplication_token = 'test1';
INSERT INTO test SELECT number%3, 2 FROM numbers(9) SETTINGS insert_deduplication_token = 'test2';
INSERT INTO test SELECT number%3, 2 FROM numbers(9) SETTINGS insert_deduplication_token = 'test2';

select 
  (select count() from test),
  (select sum(c) from test_mv_a),
  (select sum(c) from test_mv_b),
  (select sum(c) from test_mv_c);

select 'deduplicate_blocks_in_dependent_materialized_views=1, insert_deduplication_token = yes, results consitent';

drop table if exists test  sync;
drop table if exists test_mv_a sync;
drop table if exists test_mv_b sync;
drop table if exists test_mv_c sync;

set deduplicate_blocks_in_dependent_materialized_views=1;

CREATE TABLE test (A Int64, B Int64) ENGINE = ReplicatedMergeTree ('/clickhouse/test/tables/test','1') ORDER BY tuple()  ;

CREATE MATERIALIZED VIEW test_mv_a Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_a','1') 
order by tuple()  AS SELECT A, count() c FROM test group by A;

CREATE MATERIALIZED VIEW test_mv_b Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_b','1') 
partition by A order by tuple()  AS SELECT A, count() c FROM test group by A;

CREATE MATERIALIZED VIEW test_mv_c Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_c','1') 
order by tuple()  AS SELECT A, count() c FROM test group by A;


SET max_partitions_per_insert_block = 1;
INSERT INTO test SELECT number%3, 1 FROM numbers(9) SETTINGS insert_deduplication_token = 'test1' ; -- { serverError 252 }
SET max_partitions_per_insert_block = 0;
INSERT INTO test SELECT number%3, 1 FROM numbers(9) SETTINGS insert_deduplication_token = 'test1';
INSERT INTO test SELECT number%3, 2 FROM numbers(9) SETTINGS insert_deduplication_token = 'test2';
INSERT INTO test SELECT number%3, 2 FROM numbers(9) SETTINGS insert_deduplication_token = 'test2';

select 
  (select count() from test),
  (select sum(c) from test_mv_a),
  (select sum(c) from test_mv_b),
  (select sum(c) from test_mv_c);

drop table if exists test  sync;
drop table if exists test_mv_a sync;
drop table if exists test_mv_b sync;
drop table if exists test_mv_c sync;
