-- Tags: shard

-- Lambda capture DAGs reference constants from the enclosing DAG as INPUT nodes that carry a
-- pre-set constant column (see `addInputConstantColumnIfNecessary` in `PlannerActionsVisitor`).
-- That constant column must survive query plan serialization: functions that require constant
-- arguments (e.g. `tupleElement` produced by `t.2`) are re-resolved when the plan is deserialized
-- on the remote shard, and fail with ILLEGAL_TYPE_OF_ARGUMENT otherwise.
--
-- The array must depend on the row so the lambda is evaluated on the shard (a fully constant
-- array would be folded away before serialization).

SET serialize_query_plan = 1, prefer_localhost_replica = 0;

-- { echo }
SELECT arrayMap(t -> t.2, [(number, 'a'), (number + 1, 'b')]) AS x FROM remote('127.0.0.1', numbers(3)) ORDER BY x;
SELECT arrayMap(t -> t.1, [(number, 'a'), (number + 1, 'b')]) AS x FROM remote('127.0.0.1', numbers(3)) ORDER BY x;
SELECT arrayFilter(t -> t.2 = 'b', [(number, 'a'), (number + 1, 'b')]) AS x FROM remote('127.0.0.1', numbers(3)) ORDER BY x;
SELECT arraySort(arrayMap(t -> t.2, groupArray((number, toString(number))))) AS x FROM remote('127.0.0.1', numbers(3)) GROUP BY number % 2 ORDER BY x;
