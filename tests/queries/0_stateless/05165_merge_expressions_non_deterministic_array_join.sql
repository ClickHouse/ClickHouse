-- The expression-merge optimization must not fuse a non-deterministic expression into a DAG that
-- contains `arrayJoin`: merged, the value is computed once per source row and then replicated across
-- the rows the `arrayJoin` expands, instead of being drawn once per output row.

SELECT 'one draw per output row';
SELECT uniqExact(r), count() FROM (SELECT rand64() AS r FROM (SELECT arrayJoin(range(4)) AS x FROM numbers(100)));
SELECT uniqExact(id), count() FROM (SELECT generateUUIDv4() AS id FROM (SELECT arrayJoin(range(4)) AS x FROM numbers(100)));

SELECT 'the same without the optimization';
SELECT uniqExact(r), count() FROM (SELECT rand64() AS r FROM (SELECT arrayJoin(range(4)) AS x FROM numbers(100)))
SETTINGS query_plan_merge_expressions = 0;

SELECT 'a filter over the expanded rows';
SELECT count() FROM (SELECT x FROM (SELECT arrayJoin(range(4)) AS x FROM numbers(100)) WHERE rand64() % 1 = 0);

SELECT 'deterministic in the query: one value for every row';
SELECT uniqExact(t), count() FROM (SELECT now() AS t FROM (SELECT arrayJoin(range(4)) AS x FROM numbers(100)));
