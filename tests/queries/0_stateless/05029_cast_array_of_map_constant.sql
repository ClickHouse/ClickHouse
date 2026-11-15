-- Tags: distributed

-- A `Map` is physically an `Array(Tuple(key, value))`, and that is how a `Map` constant is written
-- down when the analyzer serializes a query for a remote server: `ConstantNode::toASTImpl` renders
-- the field as an array of tuples and wraps it into `_CAST(..., 'Array(Map(K, V))')`. `CAST` used to
-- reject that because it counted the nesting depth of the two shapes differently.

SELECT _CAST([[('k', 'v')]], 'Array(Map(String, String))');
SELECT _CAST([[('k', 'v'), ('k2', 'v2')], [('k3', 'v3')]], 'Array(Map(String, String))');
SELECT _CAST([[('k', NULL)]], 'Array(Map(String, Nullable(Nothing)))');
SELECT _CAST([[[('k', 'v')]]], 'Array(Array(Map(String, String)))');
SELECT _CAST([], 'Array(Map(String, String))');
SELECT _CAST([[]], 'Array(Map(String, String))');

-- The opposite direction is accepted as well, matching the top-level `Map` to `Array(Tuple)` cast.
SELECT _CAST([map('k', 'v')], 'Array(Array(Tuple(String, String)))');

-- A genuine nesting mismatch is still rejected.
SELECT _CAST(['v'], 'Array(Map(String, String))'); -- { serverError TYPE_MISMATCH }
SELECT _CAST([[('k', 'v')]], 'Array(Array(Map(String, String)))'); -- { serverError TYPE_MISMATCH }
SELECT _CAST([['v']], 'Array(String)'); -- { serverError TYPE_MISMATCH }

DROP TABLE IF EXISTS t_array_of_map_constant;
CREATE TABLE t_array_of_map_constant (m Map(String, String)) ENGINE = MergeTree ORDER BY m;
INSERT INTO t_array_of_map_constant SELECT map('k', toString(number)) FROM numbers(4);

-- The constant is folded on the initiator and has to survive the trip to the shard.
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_array_of_map_constant) WHERE has([map('k', '3')], m);
SELECT sum(indexOf([map('k', '1'), map('k', '3')], m)) FROM remote('127.0.0.{1,2}', currentDatabase(), t_array_of_map_constant);

DROP TABLE t_array_of_map_constant;
