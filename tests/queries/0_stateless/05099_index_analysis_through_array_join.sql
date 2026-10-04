-- Tags: no-parallel-replicas
-- A condition on an ARRAY JOIN element is used for skip indexes like `has(col, x)` is.

DROP TABLE IF EXISTS t_aj_index;

CREATE TABLE t_aj_index (id UInt64, tags Array(String), INDEX idx_tags tags TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- unique tags, 13 granules
INSERT INTO t_aj_index SELECT number, [concat('tag_', toString(number))] FROM numbers(100000);

-- baseline: the matching granule plus one bloom filter false positive
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_aj_index WHERE hasAny(tags, ['tag_42'])) WHERE explain ILIKE '%Granules: 2/13%';

-- the clause form prunes the same way
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_aj_index ARRAY JOIN tags AS t WHERE t IN ('tag_42')) WHERE explain ILIKE '%Granules: 2/13%';
SELECT count() FROM t_aj_index ARRAY JOIN tags AS t WHERE t IN ('tag_42') SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_aj_index ARRAY JOIN tags AS t WHERE t IN ('tag_42') SETTINGS use_skip_indexes = 0;

-- the lowered function form keeps its pruning
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_aj_index WHERE arrayJoin(tags) = 'tag_42' SETTINGS query_plan_lower_array_join_function = 1) WHERE explain ILIKE '%Granules: 2/13%';
SELECT count() FROM t_aj_index WHERE arrayJoin(tags) = 'tag_42' SETTINGS query_plan_lower_array_join_function = 1;

-- a passenger conjunct pushed below the join must not hide the element one from the index
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_aj_index ARRAY JOIN tags AS t WHERE t IN ('tag_42') AND id > 40) WHERE explain ILIKE '%Granules: 2/13%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_aj_index WHERE arrayJoin(tags) = 'tag_42' AND id > 40 SETTINGS query_plan_lower_array_join_function = 1) WHERE explain ILIKE '%Granules: 2/13%';
SELECT count() FROM t_aj_index ARRAY JOIN tags AS t WHERE t IN ('tag_42') AND id > 40;

-- LEFT pads empty arrays with defaults, so no index
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_aj_index LEFT ARRAY JOIN tags AS t WHERE t IN ('tag_42')) WHERE explain ILIKE '%Name: idx_tags%';

DROP TABLE t_aj_index;

-- the LIMIT must not reach the source: the first 90 arrays are empty
SELECT count() FROM (SELECT arrayJoin(if(number < 90, [], [number])) FROM numbers(100) LIMIT 3) SETTINGS query_plan_lower_array_join_function = 1;
