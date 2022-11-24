CREATE TABLE source (a Int32) Engine=MergeTree() ORDER BY tuple();

CREATE MATERIALIZED VIEW mv_a Engine=MergeTree() order by tuple() POPULATE as
select 1 as a,
(select count() from source) as count,
(select min(a) from source) as min,
(select max(a) from source) as max
from source;

SET allow_experimental_analyzer = 1;

insert into source select number from numbers(2000) SETTINGS min_insert_block_size_rows=1500,
max_insert_block_size=1500, max_insert_threads=0, max_block_size=10, parallel_view_processing=0,
max_threads=1, use_index_for_in_with_subqueries=0;

SELECT DISTINCT a, count, min, max from mv_a;
SELECT count() from mv_a;