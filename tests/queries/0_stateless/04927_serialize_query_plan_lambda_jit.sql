-- https://github.com/ClickHouse/ClickHouse/issues/115310
-- A lambda body containing AND with a comparison constant wider than UInt8 could get
-- JIT-fused (LLVMFunction), whose getName() returns a dump of the fused sub-expression
-- rather than a real function name. ActionsDAG::serialize() wrote that dump verbatim for
-- distributed execution, and the receiving node failed with UNKNOWN_FUNCTION trying to
-- resolve it as a real function. serialize() now decompiles any JIT-fused node back into
-- its real, resolvable sub-functions before writing it out, so expression compilation
-- stays fully enabled everywhere -- the shipped plan just never contains a fused node.

DROP TABLE IF EXISTS t_04927_map_dist;
DROP TABLE IF EXISTS t_04927_map;

CREATE TABLE t_04927_map (id UInt64, col Map(String, UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_04927_map SELECT number, map('key' || toString(number % 3), number * 100) FROM numbers(10);
CREATE TABLE t_04927_map_dist AS t_04927_map ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_04927_map);

-- was: Code 46, UNKNOWN_FUNCTION -- must now return 10 rows with the correct 3 matches
SELECT mapExists((k, v) -> k LIKE '%2' AND v < 1000, col)
FROM t_04927_map_dist
ORDER BY id
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 1;

-- same, with compile_expressions forced on and the compile threshold forced to 0 --
-- the most aggressive JIT-triggering settings possible -- must still not break
SELECT mapExists((k, v) -> k LIKE '%2' AND v < 1000, col)
FROM t_04927_map_dist
ORDER BY id
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 1, compile_expressions = 1, min_count_to_compile_expression = 0;

-- negative control: with serialize_query_plan=0, expression compilation must still work
-- normally (this fix must not disable JIT globally, only when a plan might be shipped)
SELECT count() FROM
(
    SELECT mapExists((k, v) -> k LIKE '%2' AND v < 1000, col) AS r
    FROM t_04927_map_dist
    SETTINGS serialize_query_plan = 0, compile_expressions = 1, min_count_to_compile_expression = 0
)
WHERE r = 1;

DROP TABLE t_04927_map_dist;
DROP TABLE t_04927_map;
