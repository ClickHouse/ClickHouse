-- Tags: no-random-settings, no-random-merge-tree-settings

-- LowCardinality(String) VALUES (not keys — LC keys are rejected) with the
-- `with_key_columns` Map serialization: insert / merge / read correctness.

DROP TABLE IF EXISTS t_lc_value;
CREATE TABLE t_lc_value
(
    id UInt32,
    m Map(String, LowCardinality(String))
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';

SYSTEM STOP MERGES t_lc_value;
INSERT INTO t_lc_value VALUES (1, {'a': 'x', 'b': 'y'}), (2, {'a': 'z'});
INSERT INTO t_lc_value VALUES (3, {'b': 'y', 'c': 'w'}), (4, {});

SELECT id, m FROM t_lc_value ORDER BY id;
SELECT id, m['a'], m['b'], m['c'], m['zzz'] FROM t_lc_value ORDER BY id;
SELECT id, mapContains(m, 'a'), mapContains(m, 'b'), mapContains(m, 'c') FROM t_lc_value ORDER BY id;
SELECT id, mapKeys(m), mapValues(m) FROM t_lc_value ORDER BY id;

SYSTEM START MERGES t_lc_value;
OPTIMIZE TABLE t_lc_value FINAL;

SELECT 'after merge';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_lc_value' AND active;
SELECT id, m FROM t_lc_value ORDER BY id;
SELECT id, m['a'], m['b'], m['c'], m['zzz'] FROM t_lc_value ORDER BY id;
SELECT id, mapContains(m, 'a'), mapContains(m, 'b'), mapContains(m, 'c') FROM t_lc_value ORDER BY id;
SELECT id, mapKeys(m), mapValues(m) FROM t_lc_value ORDER BY id;
SELECT groupUniqArrayArray(mapKeys(m)) FROM t_lc_value;

DROP TABLE t_lc_value;
