-- Correlated EXISTS with a RIGHT JOIN on a JSON key executed with grace_hash and several initial
-- buckets used to throw a logical error `Invalid state transition, expected WRITING_BLOCKS, got
-- JOINING_BLOCKS`: the weak hash of a JSON column depended on the internal path map order, so the
-- same row could be scattered to different buckets before and after the spill round-trip through
-- disk. https://github.com/ClickHouse/ClickHouse/issues/112867

SET enable_json_type = 1, allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t_json_grace;
CREATE TABLE t_json_grace (json JSON(a Array(UInt32), b Array(UInt32), c UInt32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_grace SELECT toJSONString(map('a', range(number % 3 + 1), 'b', range(number % 2 + 1), 'c', number)) FROM numbers(10);

SELECT o.json FROM t_json_grace AS o
WHERE EXISTS (SELECT 1 FROM t_json_grace AS i RIGHT JOIN t_json_grace AS j ON i.json = j.json WHERE i.json = o.json)
ORDER BY o.json.c
SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 4, max_threads = 1;

DROP TABLE t_json_grace;
