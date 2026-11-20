-- Multi-key GROUP BY over LowCardinality keys caches aggregate states by the linearized tuple of
-- dictionary positions. Distinct key tuples must never share a position, and a cache hit must return
-- the state of the same key: an aliasing bug merged groups and changed min()/max() while sums stayed
-- constant. Forced external aggregation with several threads is the shape that exposed it.

DROP TABLE IF EXISTS lc_group_by_position_cache;
CREATE TABLE lc_group_by_position_cache
(
    s LowCardinality(String),
    f LowCardinality(FixedString(4)),
    v UInt64
)
ENGINE = MergeTree ORDER BY v;

-- Four parts with shifted value orders so every part builds its dictionaries differently.
INSERT INTO lc_group_by_position_cache SELECT concat('h', toString((number + 0) % 100)), toFixedString(toString((number + 0) % 2), 4), number FROM numbers(50000);
INSERT INTO lc_group_by_position_cache SELECT concat('h', toString((number + 7) % 100)), toFixedString(toString((number + 3) % 2), 4), number FROM numbers(50000);
INSERT INTO lc_group_by_position_cache SELECT concat('h', toString((number + 14) % 100)), toFixedString(toString((number + 6) % 2), 4), number FROM numbers(50000);
INSERT INTO lc_group_by_position_cache SELECT concat('h', toString((number + 21) % 100)), toFixedString(toString((number + 9) % 2), 4), number FROM numbers(50000);

SELECT s, f, count(), sum(v), min(v), max(v) FROM lc_group_by_position_cache GROUP BY s, f ORDER BY s, f SETTINGS max_bytes_before_external_group_by = 1, max_threads = 8, enable_adaptive_aggregator = 0;
SELECT s, f, count(), sum(v), min(v), max(v) FROM lc_group_by_position_cache GROUP BY s, f ORDER BY s, f SETTINGS max_bytes_before_external_group_by = 1000000000, max_threads = 8, enable_adaptive_aggregator = 0;
SELECT s, f, count(), sum(v), min(v), max(v) FROM lc_group_by_position_cache GROUP BY s, f ORDER BY s, f SETTINGS max_bytes_before_external_group_by = 1, max_threads = 8;
SELECT s, f, count() FROM lc_group_by_position_cache GROUP BY s, f ORDER BY s, f SETTINGS max_bytes_before_external_group_by = 1, max_threads = 8, enable_adaptive_aggregator = 0;

DROP TABLE IF EXISTS lc_group_by_position_cache;
