-- The automatic statistics part pruner emits its own Granules: line, and auto_statistics_types is
-- randomized, so the EXPLAIN assertions below would match the wrong entry without this.
SET use_statistics_for_part_pruning = 0;

CREATE TABLE arr (id UInt64, a Array(Tuple(Float64, Bool)), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO arr VALUES (1, [(1., false)]), (2, [(1., false)]), (3, [(1., false)]);
INSERT INTO arr VALUES (4, [(1., true)]), (5, [(1., true)]), (6, [(1., true)]);

-- SerializationMap reads a Field through its own body, not through SerializationArray.
CREATE TABLE m (id UInt64, a Map(String, Bool), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO m VALUES (1, map('k', false)), (2, map('k', false)), (3, map('k', false));
INSERT INTO m VALUES (4, map('k', true)), (5, map('k', true)), (6, map('k', true));

CREATE TABLE s (id UInt64, f Bool, INDEX idx_f f TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO s VALUES (1, false), (2, false), (3, false);
INSERT INTO s VALUES (4, true), (5, true), (6, true);

-- Part-level minmax over a partition key. DETACH + ATTACH is required: without it the bounds are
-- still the writer-produced ones held in memory and MinMaxIndex::load never runs.
CREATE TABLE p (id UInt64, a Array(Bool)) ENGINE = MergeTree PARTITION BY a ORDER BY id
SETTINGS index_granularity = 3, min_bytes_for_wide_part = 0;
INSERT INTO p VALUES (1, [false]), (2, [false]), (3, [false]);
INSERT INTO p VALUES (4, [true]), (5, [true]), (6, [true]);

CREATE TABLE pc (id UInt64, a Array(UInt8)) ENGINE = MergeTree PARTITION BY a ORDER BY id
SETTINGS index_granularity = 3, min_bytes_for_wide_part = 0;
INSERT INTO pc VALUES (1, [0]), (2, [0]), (3, [0]);
INSERT INTO pc VALUES (4, [1]), (5, [1]), (6, [1]);

DETACH TABLE p; ATTACH TABLE p;
DETACH TABLE pc; ATTACH TABLE pc;

-- ORDER BY takes its bounds from primary.cidx through columns, so no Field is read back on this
-- path and only the query-side constant can carry the other representation. `ora` has no key on a.
CREATE TABLE ora (id UInt64, a Array(Nullable(Bool))) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO ora VALUES (1, [false]), (2, [false]), (3, [false]), (4, [true]), (5, [true]), (6, [true]);

CREATE TABLE pk (id UInt64, a Array(Nullable(Bool))) ENGINE = MergeTree ORDER BY (a, id)
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_nullable_key = 1;
INSERT INTO pk VALUES (1, [false]), (2, [false]), (3, [false]), (4, [true]), (5, [true]), (6, [true]);

-- A nullable element also moves the query-side constant to the other representation, so each bound
-- direction is decided by a different side of the comparison and both are needed.
CREATE TABLE nu (id UInt64, a Array(Nullable(Bool)), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO nu VALUES (1, [false]), (2, [false]), (3, [false]);
INSERT INTO nu VALUES (4, [true]), (5, [true]), (6, [true]);

CREATE TABLE mnu (id UInt64, a Map(String, Nullable(Bool)), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO mnu VALUES (1, map('k', false)), (2, map('k', false)), (3, map('k', false));
INSERT INTO mnu VALUES (4, map('k', true)), (5, map('k', true)), (6, map('k', true));

-- `j` declares no paths, so `a` is a dynamic path, and a dynamic path is the one place inside a
-- container where both sides of the comparison already agree. These arms pin that range analysis
-- stays unchanged there. Only the >= direction ever returned the wrong rows, so a <=-only arm
-- would not have caught it.
CREATE TABLE oj (id UInt64, j JSON, INDEX idx_j j TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         allow_minmax_index_for_json = 1;
INSERT INTO oj VALUES (1, '{"a": false}'), (2, '{"a": false}'), (3, '{"a": false}');
INSERT INTO oj VALUES (4, '{"a": true}'), (5, '{"a": true}'), (6, '{"a": true}');

-- Top-K reads the bound through a third granule reader and compares it against a threshold taken
-- from live column data, so a scalar Bool is enough here. Every granule holds both values, so
-- neither sort direction can be answered from the first granule read: LIMIT 6 selects all 6
-- granules and max_block_size = 8 makes the threshold appear once the first one has been read.
CREATE TABLE tk (id UInt64, f Bool, INDEX idx_f f TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO tk SELECT number, number % 2 = 0 FROM numbers(48);

CREATE TABLE tk8 (id UInt64, f UInt8, INDEX idx_f f TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO tk8 SELECT number, number % 2 = 0 FROM numbers(48);

-- Granule 0 holds both values, granule 1 is all false, granules 2-5 are all true.
CREATE TABLE tkw (id UInt64, f Bool, INDEX idx_f f TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO tkw SELECT number, (number >= 4 AND number < 8) OR number >= 16 FROM numbers(48);

SELECT 'array', count(), sum(id) FROM arr WHERE a <= [(1., false)];
SELECT 'array noidx', count(), sum(id) FROM arr WHERE a <= [(1., false)] SETTINGS use_skip_indexes = 0;
SELECT 'map', count(), sum(id) FROM m WHERE a <= map('k', false);
SELECT 'map noidx', count(), sum(id) FROM m WHERE a <= map('k', false) SETTINGS use_skip_indexes = 0;
SELECT 'scalar', count(), sum(id) FROM s WHERE f <= false;
SELECT 'scalar noidx', count(), sum(id) FROM s WHERE f <= false SETTINGS use_skip_indexes = 0;
SELECT 'partition', count(), sum(id) FROM p WHERE a <= [false];
SELECT 'partition noidx', count(), sum(id) FROM p WHERE a <= [false] SETTINGS use_skip_indexes = 0;
SELECT 'partition uint8', count(), sum(id) FROM pc WHERE a <= [0];
SELECT 'partition uint8 noidx', count(), sum(id) FROM pc WHERE a <= [0] SETTINGS use_skip_indexes = 0;
SELECT 'order by =', sum(id) FROM pk WHERE a = [false];
SELECT 'order by = oracle', sum(id) FROM ora WHERE a = [false];
SELECT 'order by >=', sum(id) FROM pk WHERE a >= [false];
SELECT 'order by >= oracle', sum(id) FROM ora WHERE a >= [false];
SELECT 'nullable <=', count(), sum(id) FROM nu WHERE a <= [false];
SELECT 'nullable <= noidx', count(), sum(id) FROM nu WHERE a <= [false] SETTINGS use_skip_indexes = 0;
SELECT 'nullable >=', count(), sum(id) FROM nu WHERE a >= [false];
SELECT 'nullable >= noidx', count(), sum(id) FROM nu WHERE a >= [false] SETTINGS use_skip_indexes = 0;
SELECT 'nullable map >=', count(), sum(id) FROM mnu WHERE a >= map('k', false);
SELECT 'nullable map >= noidx', count(), sum(id) FROM mnu WHERE a >= map('k', false) SETTINGS use_skip_indexes = 0;
SELECT 'json >=', count(), sum(id) FROM oj WHERE j >= '{"a": false}';
SELECT 'json >= noidx', count(), sum(id) FROM oj WHERE j >= '{"a": false}' SETTINGS use_skip_indexes = 0;
SELECT 'json <=', count(), sum(id) FROM oj WHERE j <= '{"a": false}';
SELECT 'json <= noidx', count(), sum(id) FROM oj WHERE j <= '{"a": false}' SETTINGS use_skip_indexes = 0;

-- All five settings below are randomized, and each one decides whether these arms reach the
-- comparison at all - including query_plan_max_limit_for_top_k_optimization, which is drawn from a
-- set containing 1 and then refuses the rewrite because the LIMIT is larger. The two row limits are
-- pinned to their default 0 as well: the stateless-test user profile sets max_rows_to_read, and a
-- throwing row limit turns off skip indexes on data read, which is the path the granule comparison
-- below runs on (see 04812_row_policy_top_k_optimization). The clause belongs on the outer query:
-- the top-K rewrite is a plan-level optimization and reads these from the outer context, so a
-- SETTINGS clause on the subquery alone leaves the arms silently vacuous.
SELECT 'top-k asc', groupArray(f) FROM (SELECT f FROM tk ORDER BY f ASC LIMIT 6)
  SETTINGS max_block_size = 8, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1, use_top_k_dynamic_filtering = 1,
           query_plan_max_limit_for_top_k_optimization = 0, max_rows_to_read = 0, max_rows_to_read_leaf = 0;
SELECT 'top-k asc noidx', groupArray(f) FROM (SELECT f FROM tk ORDER BY f ASC LIMIT 6)
  SETTINGS max_block_size = 8, use_skip_indexes_for_top_k = 0, use_skip_indexes_on_data_read = 1, use_top_k_dynamic_filtering = 1,
           query_plan_max_limit_for_top_k_optimization = 0, max_rows_to_read = 0, max_rows_to_read_leaf = 0;
SELECT 'top-k desc', groupArray(f) FROM (SELECT f FROM tk ORDER BY f DESC LIMIT 6)
  SETTINGS max_block_size = 8, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1, use_top_k_dynamic_filtering = 1,
           query_plan_max_limit_for_top_k_optimization = 0, max_rows_to_read = 0, max_rows_to_read_leaf = 0;
SELECT 'top-k desc noidx', groupArray(f) FROM (SELECT f FROM tk ORDER BY f DESC LIMIT 6)
  SETTINGS max_block_size = 8, use_skip_indexes_for_top_k = 0, use_skip_indexes_on_data_read = 1, use_top_k_dynamic_filtering = 1,
           query_plan_max_limit_for_top_k_optimization = 0, max_rows_to_read = 0, max_rows_to_read_leaf = 0;
SELECT 'top-k uint8 asc', groupArray(f) FROM (SELECT f FROM tk8 ORDER BY f ASC LIMIT 6)
  SETTINGS max_block_size = 8, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1, use_top_k_dynamic_filtering = 1,
           query_plan_max_limit_for_top_k_optimization = 0, max_rows_to_read = 0, max_rows_to_read_leaf = 0;
SELECT 'top-k uint8 desc', groupArray(f) FROM (SELECT f FROM tk8 ORDER BY f DESC LIMIT 6)
  SETTINGS max_block_size = 8, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1, use_top_k_dynamic_filtering = 1,
           query_plan_max_limit_for_top_k_optimization = 0, max_rows_to_read = 0, max_rows_to_read_leaf = 0;

-- The index must still prune, otherwise the rows above would be correct only because it stopped working.
SELECT 'array prunes', count() > 0 FROM (EXPLAIN indexes = 1, actions = 0 SELECT sum(id) FROM arr WHERE a <= [(1., false)]) WHERE explain LIKE '%Granules: 1/2%';
SELECT 'map prunes', count() > 0 FROM (EXPLAIN indexes = 1, actions = 0 SELECT sum(id) FROM m WHERE a <= map('k', false)) WHERE explain LIKE '%Granules: 1/2%';
SELECT 'scalar prunes', count() > 0 FROM (EXPLAIN indexes = 1, actions = 0 SELECT sum(id) FROM s WHERE f = false) WHERE explain LIKE '%Granules: 1/2%';
-- Part-level minmax and partition pruning are redundant over the same key, and the `Parts:`
-- denominators are chained, so whichever of them declines the other renders `Parts: 1/2` instead.
-- Requiring that no entry passed both parts through pins the part-level one.
SELECT 'partition prunes', count() = 0 FROM (EXPLAIN indexes = 1, actions = 0 SELECT sum(id) FROM p WHERE a <= [false]) WHERE explain LIKE '%Parts: 2/2%';
-- A negative assertion is also satisfied by absence, so the part-level entry is pinned as present too.
SELECT 'partition minmax entry', count() > 0 FROM (EXPLAIN indexes = 1, actions = 0 SELECT sum(id) FROM p WHERE a <= [false]) WHERE explain LIKE '%Min-Max%';
SELECT 'pk prunes', count() > 0 FROM (EXPLAIN indexes = 1, actions = 0 SELECT sum(id) FROM pk WHERE a = [false]) WHERE explain LIKE '%Granules: 1/2%';
SELECT 'json prunes', count() > 0 FROM (EXPLAIN indexes = 1, actions = 0 SELECT sum(id) FROM oj WHERE j <= '{"a": false}') WHERE explain LIKE '%Granules: 1/2%';
-- The two Nullable-element fixtures deliberately carry no pruning arm: inside a container a nested
-- NULL orders by its Field tag while the data orders it last, so pruning there is not sound to pin.

-- LIMIT 8 is above the granule count, so every granule is selected and only the runtime threshold
-- can skip one. The row count is the only observable separating an active threshold filter from one
-- that declines, since a decline answers correctly; the bounds are asserted too because a filter
-- that skips too much also reads fewer rows. max_threads, enable_parallel_replicas and
-- ast_fuzzer_runs are pinned so the count comes from one local reader with one log row.
SELECT 'top-k wide', count(), min(f), max(f) FROM (SELECT f FROM tkw ORDER BY f ASC LIMIT 8)
  SETTINGS max_block_size = 8, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1, use_top_k_dynamic_filtering = 1,
           query_plan_max_limit_for_top_k_optimization = 0, max_threads = 1, enable_parallel_replicas = 0,
           max_rows_to_read = 0, max_rows_to_read_leaf = 0, ast_fuzzer_runs = 0, log_comment = '05046_tkw';
SYSTEM FLUSH LOGS query_log;
SELECT 'top-k wide skips', read_rows < 48 FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '05046_tkw';
