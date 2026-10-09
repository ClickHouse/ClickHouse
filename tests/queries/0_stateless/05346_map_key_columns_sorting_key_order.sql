-- `with_key_columns` reads the keys of a Map back sorted within each row. The keys are
-- sorted before the sorting/partition keys, indices and projections are computed, so
-- they describe the stored data.

SET allow_experimental_map_key_columns = 1;

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_sort;
DROP TABLE IF EXISTS t_part;
DROP TABLE IF EXISTS t_mut;

CREATE TABLE t_sort (id UInt8, m Map(String, UInt64), t Tuple(x Map(String, UInt64)))
ENGINE = MergeTree ORDER BY (id, m, t)
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types',
    propagate_types_serialization_versions_to_nested_types = 1, min_bytes_for_wide_part = 0, index_granularity = 1;

INSERT INTO t_sort SELECT 1, map('b', 1, 'a', 1), tuple(map('d', 1, 'c', 2));
INSERT INTO t_sort SELECT 1, map('a', 5), tuple(map('c', 1));
INSERT INTO t_sort SELECT 1, map('b', 1, 'a', 1), tuple(map('c', 9));

SELECT 'in order', id, m, t FROM t_sort ORDER BY id, m, t SETTINGS optimize_read_in_order = 1;
SELECT 'full sort', id, m, t FROM t_sort ORDER BY id, m, t SETTINGS optimize_read_in_order = 0;
SELECT 'pk', count() FROM t_sort WHERE m < map('a', 2);
SELECT 'no pk', count() FROM t_sort WHERE m < map('a', 2) SETTINGS use_primary_key = 0;

OPTIMIZE TABLE t_sort FINAL;
SELECT 'merged', id, m, t FROM t_sort ORDER BY id, m, t SETTINGS optimize_read_in_order = 1;
SELECT 'merged pk', count() FROM t_sort WHERE (id, m, t) < (1, map('a', 1, 'b', 1), tuple(map('c', 5)));
SELECT 'merged no pk', count() FROM t_sort WHERE (id, m, t) < (1, map('a', 1, 'b', 1), tuple(map('c', 5))) SETTINGS use_primary_key = 0;

CREATE TABLE t_part (k UInt8, m Map(String, UInt64))
ENGINE = MergeTree PARTITION BY mapKeys(m)[1] ORDER BY k
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types';

INSERT INTO t_part SELECT 1, map('b', 1, 'a', 1);
SELECT 'partition', _partition_id = partitionId(mapKeys(m)[1]), mapKeys(m)[1] FROM t_part;

ALTER TABLE t_part ADD COLUMN d Map(String, UInt8) DEFAULT map('y', 1, 'x', 2);
SELECT 'default', d, d.keys, d['y'] FROM t_part;

CREATE TABLE t_mut (id UInt8, m Map(String, UInt64),
    INDEX i m TYPE set(10) GRANULARITY 1,
    PROJECTION p (SELECT m, count() GROUP BY m))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0, index_granularity = 1;

INSERT INTO t_mut SELECT number, map('a', number) FROM numbers(4);
ALTER TABLE t_mut UPDATE m = map('z', 1, 'a', 0) WHERE id = 1;

SELECT 'index', count() FROM t_mut WHERE m = map('a', 0, 'z', 1);
SELECT 'no index', count() FROM t_mut WHERE m = map('a', 0, 'z', 1) SETTINGS use_skip_indexes = 0;
SELECT 'projection', m, count() FROM t_mut GROUP BY m ORDER BY m SETTINGS force_optimize_projection_name = 'p';
SELECT 'no projection', m, count() FROM t_mut GROUP BY m ORDER BY m SETTINGS optimize_use_projections = 0;

DROP TABLE t_sort;
DROP TABLE t_part;
DROP TABLE t_mut;
