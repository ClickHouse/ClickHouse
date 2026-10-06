-- Tags: no-random-merge-tree-settings
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;
SET parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;
SET enable_analyzer=1;

DROP TABLE IF EXISTS tt0;
CREATE TABLE tt0 (k UInt64, v String, blob String, PROJECTION proj_v (select * order by v)) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO tt0 SELECT number, toString(number), repeat('blob_', number % 10) FROM numbers(1_000_000);

select '-- no projection';
select trimLeft(explain) as s from (EXPLAIN SELECT * FROM tt0 ORDER BY k ASC LIMIT 10) where s ilike 'LazilyRead%';

SELECT * FROM tt0 ORDER BY k ASC LIMIT 10;

select '-- projection';
select trimLeft(explain) as s from (EXPLAIN SELECT * FROM tt0 WHERE v = '3' ORDER BY v ASC LIMIT 10) where s ilike 'ReadFromMergeTree (proj_v)';
select trimLeft(explain) as s from (EXPLAIN SELECT * FROM tt0 WHERE v = '3' ORDER BY v ASC LIMIT 10) where s ilike 'LazilyRead%';

SELECT * FROM tt0 WHERE v = '3' ORDER BY v ASC LIMIT 10;

-- create table which has parts with and without projection
select '-- mixed reading';
DROP TABLE IF EXISTS tt1;
CREATE TABLE tt1 (k UInt64, v String, blob String) ENGINE=MergeTree() ORDER BY tuple() settings index_granularity=10;
SYSTEM STOP MERGES tt1;
INSERT INTO tt1 SELECT number, toString(number), repeat('blob_', number % 10) FROM numbers(1_000);

ALTER TABLE tt1 ADD PROJECTION proj_v (select * order by v);
INSERT INTO tt1 SELECT number, toString(number), repeat('blob_', number % 10) FROM numbers(1_000, 1_000);

-- check that table has 2 parts without and with projection
select name, projections from system.parts where database = currentDatabase() and table = 'tt1' order by name;
-- reading using projection from the table should have 2 reading steps, - one for part w/o proj and one for part with proj
select 'Reading steps: '|| count() from (EXPLAIN SELECT * FROM tt1 WHERE v = '1001' ORDER BY v ASC LIMIT 10) where trimLeft(explain) ilike 'ReadFromMergeTree%';
-- currently lazy materialization doesn't support such mixed reading
select trimLeft(explain) as s from (EXPLAIN SELECT * FROM tt1 WHERE v = '1001' ORDER BY v ASC LIMIT 10) where s ilike 'LazilyRead%';

DROP TABLE tt1;
DROP TABLE tt0;
