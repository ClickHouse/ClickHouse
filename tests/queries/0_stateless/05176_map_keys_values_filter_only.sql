-- Map key/value filters should use the matching subcolumn even when the full Map is selected.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET optimize_rewrite_has_to_in = 0;

DROP TABLE IF EXISTS t_map_keys_values_filter_only;
DROP TABLE IF EXISTS t_map_keys_values_filter_only_buckets;
DROP TABLE IF EXISTS t_map_keys_values_filter_only_indexed;
CREATE TABLE t_map_keys_values_filter_only
(
    id UInt64,
    m Map(String, String)
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic',
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1;

INSERT INTO t_map_keys_values_filter_only VALUES
    (0, {'service': 'api', 'debug': '1'}),
    (1, {'service': 'worker'}),
    (2, {'debug': '1'}),
    (3, {}),
    (4, {'service': 'api'});

-- The full Map remains in the projection, while mapKeys is read from m.keys.
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id, m
    FROM t_map_keys_values_filter_only
    WHERE has(mapKeys(m), 'service')
)
WHERE explain LIKE '%m.keys%';

SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id, m
    FROM t_map_keys_values_filter_only
    PREWHERE has(mapKeys(m), 'service')
)
WHERE explain LIKE '%m.keys%';

-- The full Map remains in the projection, while mapValues is read from m.values.
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id, m
    FROM t_map_keys_values_filter_only
    WHERE has(mapValues(m), 'api')
)
WHERE explain LIKE '%m.values%';

SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id, m
    FROM t_map_keys_values_filter_only
    PREWHERE has(mapValues(m), 'api')
)
WHERE explain LIKE '%m.values%';

-- The optimized and unoptimized paths must return the same full Map values.
SELECT id, m
FROM t_map_keys_values_filter_only
WHERE has(mapKeys(m), 'service')
ORDER BY id;

SELECT id, m
FROM t_map_keys_values_filter_only
WHERE has(mapKeys(m), 'service')
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT id, m
FROM t_map_keys_values_filter_only
WHERE has(mapValues(m), 'api')
ORDER BY id;

SELECT id, m
FROM t_map_keys_values_filter_only
WHERE has(mapValues(m), 'api')
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

-- Bucketed Map serialization must preserve the same filter-only behavior.
CREATE TABLE t_map_keys_values_filter_only_buckets
(
    id UInt64,
    m Map(String, String)
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_coefficient = 1.0,
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1;

INSERT INTO t_map_keys_values_filter_only_buckets
SELECT id, m
FROM t_map_keys_values_filter_only;

SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id, m
    FROM t_map_keys_values_filter_only_buckets
    PREWHERE has(mapKeys(m), 'service')
)
WHERE explain LIKE '%m.keys%';

SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id, m
    FROM t_map_keys_values_filter_only_buckets
    PREWHERE has(mapValues(m), 'api')
)
WHERE explain LIKE '%m.values%';

SELECT id, m
FROM t_map_keys_values_filter_only_buckets
PREWHERE has(mapKeys(m), 'service')
ORDER BY id;

SELECT id, m
FROM t_map_keys_values_filter_only_buckets
PREWHERE has(mapValues(m), 'api')
ORDER BY id;

-- Indexed Map columns keep the existing index-aware path. The filter-only
-- subcolumn rewrite is intentionally not applied to these columns yet.
CREATE TABLE t_map_keys_values_filter_only_indexed
(
    id UInt64,
    m Map(String, String),
    INDEX idx_keys mapKeys(m) TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
    INDEX idx_values mapValues(m) TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    index_granularity = 1,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1;

INSERT INTO t_map_keys_values_filter_only_indexed VALUES
    (0, {'service': 'api', 'debug': '1'}),
    (1, {'service': 'worker'}),
    (2, {'debug': '1'}),
    (3, {}),
    (4, {'service': 'api'});

SELECT count()
FROM t_map_keys_values_filter_only_indexed
WHERE has(mapKeys(m), 'service');

SELECT count()
FROM t_map_keys_values_filter_only_indexed
WHERE has(mapValues(m), 'api');

SELECT id, m
FROM t_map_keys_values_filter_only_indexed
WHERE has(mapKeys(m), 'service')
ORDER BY id;

SELECT id, m
FROM t_map_keys_values_filter_only_indexed
WHERE has(mapValues(m), 'api')
ORDER BY id;

-- The original Map index expressions remain visible to index analysis.
SELECT count() > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT id, m
    FROM t_map_keys_values_filter_only_indexed
    WHERE has(mapKeys(m), 'service')
)
WHERE explain LIKE '%idx_keys%';

SELECT count() > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT id, m
    FROM t_map_keys_values_filter_only_indexed
    WHERE has(mapValues(m), 'api')
)
WHERE explain LIKE '%idx_values%';

DROP TABLE t_map_keys_values_filter_only;
DROP TABLE t_map_keys_values_filter_only_buckets;
DROP TABLE t_map_keys_values_filter_only_indexed;
