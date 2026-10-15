-- Tags: no-replicated-database
-- Tag no-replicated-database: hypothetical indexes are session-scoped and not replicated.
-- Min-max indexes over a JSON column with a declared Bool path return the same rows as a full scan.

-- The statistics part pruner prints its own Granules: line, and statistics materialized on INSERT
-- prune the EXPLAIN WHATIF baseline; both are randomized.
SET use_statistics_for_part_pruning = 0, materialize_statistics_on_insert = 0;

CREATE TABLE t_bool (id UInt64, j JSON(a Bool), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_bool VALUES (1, '{"a": false}'), (2, '{"a": false}'), (3, '{"a": false}');
INSERT INTO t_bool VALUES (4, '{"a": true}'), (5, '{"a": true}'), (6, '{"a": true}');

CREATE TABLE t_nullable (id UInt64, j JSON(a Nullable(Bool)), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_nullable VALUES (1, '{"a": false}'), (2, '{"a": false}'), (3, '{"a": false}');
INSERT INTO t_nullable VALUES (4, '{"a": true}'), (5, '{"a": true}'), (6, '{"a": true}');

CREATE TABLE t_array (id UInt64, j JSON(a Array(Bool)), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_array VALUES (1, '{"a": [false]}'), (2, '{"a": [false]}'), (3, '{"a": [false]}');
INSERT INTO t_array VALUES (4, '{"a": [true]}'), (5, '{"a": [true]}'), (6, '{"a": [true]}');

CREATE TABLE t_tuple (id UInt64, j JSON(a Tuple(Bool)), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_tuple VALUES (1, '{"a": [false]}'), (2, '{"a": [false]}'), (3, '{"a": [false]}');
INSERT INTO t_tuple VALUES (4, '{"a": [true]}'), (5, '{"a": [true]}'), (6, '{"a": [true]}');

CREATE TABLE t_map (id UInt64, j JSON(a Map(String, Bool)), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_map VALUES (1, '{"a": {"k": false}}'), (2, '{"a": {"k": false}}'), (3, '{"a": {"k": false}}');
INSERT INTO t_map VALUES (4, '{"a": {"k": true}}'), (5, '{"a": {"k": true}}'), (6, '{"a": {"k": true}}');

CREATE TABLE t_nested (id UInt64, j JSON(a JSON(b Bool)), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_nested VALUES (1, '{"a": {"b": false}}'), (2, '{"a": {"b": false}}'), (3, '{"a": {"b": false}}');
INSERT INTO t_nested VALUES (4, '{"a": {"b": true}}'), (5, '{"a": {"b": true}}'), (6, '{"a": {"b": true}}');

CREATE TABLE t_mixed (id UInt64, j JSON(a Bool), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_mixed VALUES (1, '{"a": false, "b": true}'), (2, '{"a": false, "b": true}'), (3, '{"a": false, "b": true}');
INSERT INTO t_mixed VALUES (4, '{"a": true, "b": false}'), (5, '{"a": true, "b": false}'), (6, '{"a": true, "b": false}');

CREATE TABLE t_array_of_json (id UInt64, j Array(JSON(a Bool)), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_array_of_json VALUES (1, ['{"a": false}']), (2, ['{"a": false}']), (3, ['{"a": false}']);
INSERT INTO t_array_of_json VALUES (4, ['{"a": true}']), (5, ['{"a": true}']), (6, ['{"a": true}']);

CREATE TABLE t_tuple_of_json (id UInt64, j Tuple(x JSON(a Bool), y UInt8), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_tuple_of_json VALUES (1, ('{"a": false}', 1)), (2, ('{"a": false}', 1)), (3, ('{"a": false}', 1));
INSERT INTO t_tuple_of_json VALUES (4, ('{"a": true}', 1)), (5, ('{"a": true}', 1)), (6, ('{"a": true}', 1));

CREATE TABLE t_map_of_json (id UInt64, j Map(String, JSON(a Bool)), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_map_of_json VALUES (1, map('k', '{"a": false}')), (2, map('k', '{"a": false}')), (3, map('k', '{"a": false}'));
INSERT INTO t_map_of_json VALUES (4, map('k', '{"a": true}')), (5, map('k', '{"a": true}')), (6, map('k', '{"a": true}'));

CREATE TABLE t_dynamic (id UInt64, j JSON, INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_dynamic VALUES (1, '{"a": false}'), (2, '{"a": false}'), (3, '{"a": false}');
INSERT INTO t_dynamic VALUES (4, '{"a": true}'), (5, '{"a": true}'), (6, '{"a": true}');

CREATE TABLE t_shared (id UInt64, j JSON(max_dynamic_paths = 0), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_shared VALUES (1, '{"a": false}'), (2, '{"a": false}'), (3, '{"a": false}');
INSERT INTO t_shared VALUES (4, '{"a": true}'), (5, '{"a": true}'), (6, '{"a": true}');

CREATE TABLE t_uint8 (id UInt64, j JSON(a UInt8), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_uint8 VALUES (1, '{"a": 0}'), (2, '{"a": 0}'), (3, '{"a": 0}');
INSERT INTO t_uint8 VALUES (4, '{"a": 1}'), (5, '{"a": 1}'), (6, '{"a": 1}');

CREATE TABLE t_int (id UInt64, j JSON, INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_int VALUES (1, '{"n": 1}'), (2, '{"n": 1}'), (3, '{"n": 1}');
INSERT INTO t_int VALUES (4, '{"n": 2}'), (5, '{"n": 2}'), (6, '{"n": 2}');

SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_low_cardinality (id UInt64, j JSON(a LowCardinality(Bool)), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_low_cardinality VALUES (1, '{"a": false}'), (2, '{"a": false}'), (3, '{"a": false}');
INSERT INTO t_low_cardinality VALUES (4, '{"a": true}'), (5, '{"a": true}'), (6, '{"a": true}');

CREATE TABLE t_simple_aggregate (id UInt64, j JSON(a SimpleAggregateFunction(any, Bool)), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_simple_aggregate VALUES (1, '{"a": false}'), (2, '{"a": false}'), (3, '{"a": false}');
INSERT INTO t_simple_aggregate VALUES (4, '{"a": true}'), (5, '{"a": true}'), (6, '{"a": true}');

CREATE TABLE t_nullable_json (id UInt64, j Nullable(JSON(a Bool)), INDEX i j TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_minmax_index_for_json = 1;
INSERT INTO t_nullable_json VALUES (1, '{"a": false}'), (2, '{"a": false}'), (3, NULL);
INSERT INTO t_nullable_json VALUES (4, '{"a": true}'), (5, '{"a": true}'), (6, '{"a": true}');

SET param_p = '{"a": false}';

SELECT 'bool <=', count(), sum(id) FROM t_bool WHERE j <= '{"a": false}';
SELECT 'bool <= noidx', count(), sum(id) FROM t_bool WHERE j <= '{"a": false}' SETTINGS use_skip_indexes = 0;
SELECT 'bool =', count(), sum(id) FROM t_bool WHERE j = '{"a": false}';
SELECT 'bool >=', count(), sum(id) FROM t_bool WHERE j >= '{"a": false}';
SELECT 'bool typed constant', count(), sum(id) FROM t_bool WHERE j <= '{"a": false}'::JSON(a Bool);
SELECT 'bool parameter', count(), sum(id) FROM t_bool WHERE j = {p:JSON(a Bool)};
SELECT 'nullable', count(), sum(id) FROM t_nullable WHERE j <= '{"a": false}';
SELECT 'array', count(), sum(id) FROM t_array WHERE j <= '{"a": [false]}';
SELECT 'tuple', count(), sum(id) FROM t_tuple WHERE j <= '{"a": [false]}';
SELECT 'tuple noidx', count(), sum(id) FROM t_tuple WHERE j <= '{"a": [false]}' SETTINGS use_skip_indexes = 0;
SELECT 'map', count(), sum(id) FROM t_map WHERE j <= '{"a": {"k": false}}';
SELECT 'nested json', count(), sum(id) FROM t_nested WHERE j <= '{"a": {"b": false}}';
SELECT 'mixed <=', count(), sum(id) FROM t_mixed WHERE j <= '{"a": false, "b": true}';
SELECT 'mixed =', count(), sum(id) FROM t_mixed WHERE j = '{"a": false, "b": true}';
SELECT 'array of json', count(), sum(id) FROM t_array_of_json WHERE j <= CAST(['{"a": false}'], 'Array(JSON(a Bool))');
SELECT 'tuple of json', count(), sum(id) FROM t_tuple_of_json WHERE j <= CAST(tuple('{"a": false}', 1), 'Tuple(x JSON(a Bool), y UInt8)');
SELECT 'map of json', count(), sum(id) FROM t_map_of_json WHERE j <= CAST(map('k', '{"a": false}'), 'Map(String, JSON(a Bool))');
SELECT 'dynamic <=', count(), sum(id) FROM t_dynamic WHERE j <= '{"a": false}';
SELECT 'dynamic >=', count(), sum(id) FROM t_dynamic WHERE j >= '{"a": false}';
SELECT 'dynamic typed constant <=', count(), sum(id) FROM t_dynamic WHERE j <= '{"a": false}'::JSON(a Bool);
SELECT 'dynamic typed constant =', count(), sum(id) FROM t_dynamic WHERE j = '{"a": false}'::JSON(a Bool);
SELECT 'dynamic typed constant noidx', count(), sum(id) FROM t_dynamic WHERE j <= '{"a": false}'::JSON(a Bool) SETTINGS use_skip_indexes = 0;
SELECT 'shared data', count(), sum(id) FROM t_shared WHERE j <= '{"a": false}';
SELECT 'uint8', count(), sum(id) FROM t_uint8 WHERE j <= '{"a": 0}';
SELECT 'int typed constant', count(), sum(id) FROM t_int WHERE j <= '{"n": 1}'::JSON(n Int64);
SELECT 'low cardinality', count(), sum(id) FROM t_low_cardinality WHERE j <= '{"a": false}';
SELECT 'simple aggregate function', count(), sum(id) FROM t_simple_aggregate WHERE j <= '{"a": false}';
SELECT 'nullable json <=', count(), sum(id) FROM t_nullable_json WHERE j <= '{"a": false}';
SELECT 'nullable json <= noidx', count(), sum(id) FROM t_nullable_json WHERE j <= '{"a": false}' SETTINGS use_skip_indexes = 0;
SELECT 'nullable json is null', count(), sum(id) FROM t_nullable_json WHERE j IS NULL;

SELECT 'bool prunes', count() > 0 FROM (EXPLAIN indexes = 1, actions = 0 SELECT sum(id) FROM t_bool WHERE j <= '{"a": false}') WHERE explain LIKE '%Granules: 1/2%';
SELECT 'dynamic prunes', count() > 0 FROM (EXPLAIN indexes = 1, actions = 0 SELECT sum(id) FROM t_dynamic WHERE j <= '{"a": false}') WHERE explain LIKE '%Granules: 1/2%';
SELECT 'dynamic typed constant prunes', count() > 0 FROM (EXPLAIN indexes = 1, actions = 0 SELECT sum(id) FROM t_dynamic WHERE j <= '{"a": false}'::JSON(a Bool)) WHERE explain LIKE '%Granules: 1/2%';
SELECT 'int typed constant prunes', count() > 0 FROM (EXPLAIN indexes = 1, actions = 0 SELECT sum(id) FROM t_int WHERE j <= '{"n": 1}'::JSON(n Int64)) WHERE explain LIKE '%Granules: 1/2%';

-- The partition key reads the whole j, so j also gets a part-level min-max index, which ATTACH reads back from disk.
CREATE TABLE p_bool (id UInt64, j JSON(a Bool)) ENGINE = MergeTree PARTITION BY (intDiv(id, 4), length(toString(j))) ORDER BY id;
INSERT INTO p_bool VALUES (1, '{"a": false}'), (2, '{"a": false}'), (3, '{"a": false}');
INSERT INTO p_bool VALUES (4, '{"a": true}'), (5, '{"a": true}'), (6, '{"a": true}');
CREATE TABLE p_dynamic (id UInt64, j JSON) ENGINE = MergeTree PARTITION BY (intDiv(id, 4), length(toString(j))) ORDER BY id;
INSERT INTO p_dynamic VALUES (1, '{"a": false}'), (2, '{"a": false}'), (3, '{"a": false}');
INSERT INTO p_dynamic VALUES (4, '{"a": true}'), (5, '{"a": true}'), (6, '{"a": true}');
CREATE TABLE p_simple_aggregate (id UInt64, j JSON(a SimpleAggregateFunction(any, Bool))) ENGINE = MergeTree PARTITION BY (intDiv(id, 4), length(toString(j))) ORDER BY id;
INSERT INTO p_simple_aggregate VALUES (1, '{"a": false}'), (2, '{"a": false}'), (3, '{"a": false}');
INSERT INTO p_simple_aggregate VALUES (4, '{"a": true}'), (5, '{"a": true}'), (6, '{"a": true}');
SELECT 'partition fresh =', count(), sum(id) FROM p_bool WHERE j = '{"a": false}';
DETACH TABLE p_bool;
ATTACH TABLE p_bool;
DETACH TABLE p_dynamic;
ATTACH TABLE p_dynamic;
DETACH TABLE p_simple_aggregate;
ATTACH TABLE p_simple_aggregate;
SELECT 'partition reloaded =', count(), sum(id) FROM p_bool WHERE j = '{"a": false}';
SELECT 'partition reloaded <=', count(), sum(id) FROM p_bool WHERE j <= '{"a": false}';
SELECT 'partition reloaded >=', count(), sum(id) FROM p_bool WHERE j >= '{"a": false}';
SELECT 'partition reloaded no pruning', count(), sum(id) FROM p_bool WHERE j <= '{"a": false}' SETTINGS use_partition_pruning = 0;
SELECT 'partition dynamic reloaded <=', count(), sum(id) FROM p_dynamic WHERE j <= '{"a": false}';
SELECT 'partition dynamic reloaded >=', count(), sum(id) FROM p_dynamic WHERE j >= '{"a": false}';
SELECT 'partition simple aggregate function reloaded <=', count(), sum(id) FROM p_simple_aggregate WHERE j <= '{"a": false}';
SELECT 'partition prunes', count() > 0 FROM (EXPLAIN indexes = 1, actions = 0 SELECT sum(id) FROM p_bool WHERE j <= '{"a": false}') WHERE explain LIKE '%Parts: 1/2%';

-- A part read back from disk merged with a new part: the merged range is written to disk.
CREATE TABLE p_merge (id UInt64, j JSON(a Bool)) ENGINE = MergeTree PARTITION BY length(toString(j)) > 0 ORDER BY id;
INSERT INTO p_merge VALUES (1, '{"a": false}'), (2, '{"a": false}'), (3, '{"a": false}');
DETACH TABLE p_merge;
ATTACH TABLE p_merge;
INSERT INTO p_merge VALUES (4, '{"a": true}'), (5, '{"a": true}'), (6, '{"a": true}');
OPTIMIZE TABLE p_merge FINAL;
SELECT 'partition merged parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 'p_merge' AND active;
SELECT 'partition merged =', count(), sum(id) FROM p_merge WHERE j = '{"a": false}';
DETACH TABLE p_merge;
ATTACH TABLE p_merge;
SELECT 'partition merged reloaded =', count(), sum(id) FROM p_merge WHERE j = '{"a": false}';

-- EXPLAIN WHATIF evaluates a hypothetical index built in memory, never written to disk.
CREATE TABLE w_dynamic (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO w_dynamic VALUES (1, '{"a": false}'), (2, '{"a": false}'), (3, '{"a": false}');
INSERT INTO w_dynamic VALUES (4, '{"a": true}'), (5, '{"a": true}'), (6, '{"a": true}');
CREATE TABLE w_bool (id UInt64, j JSON(a Bool)) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO w_bool VALUES (1, '{"a": false}'), (2, '{"a": false}'), (3, '{"a": false}');
INSERT INTO w_bool VALUES (4, '{"a": true}'), (5, '{"a": true}'), (6, '{"a": true}');
CREATE HYPOTHETICAL INDEX h ON w_dynamic (j) TYPE minmax GRANULARITY 1;
CREATE HYPOTHETICAL INDEX h ON w_bool (j) TYPE minmax GRANULARITY 1;
SELECT 'whatif dynamic', trim(explain) FROM (EXPLAIN WHATIF SELECT * FROM w_dynamic WHERE j <= '{"a": false}') WHERE explain LIKE '%skip_ratio%';
SELECT 'whatif dynamic typed constant', trim(explain) FROM (EXPLAIN WHATIF SELECT * FROM w_dynamic WHERE j <= '{"a": false}'::JSON(a Bool)) WHERE explain LIKE '%skip_ratio%';
SELECT 'whatif bool', trim(explain) FROM (EXPLAIN WHATIF SELECT * FROM w_bool WHERE j <= '{"a": false}') WHERE explain LIKE '%skip_ratio%';
