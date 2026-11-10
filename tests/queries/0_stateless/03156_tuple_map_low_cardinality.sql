DROP TABLE IF EXISTS t_map_lc;

CREATE TABLE t_map_lc
(
    id UInt64,
    t Tuple(m Map(LowCardinality(String), LowCardinality(String)))
)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_map_lc SELECT * FROM generateRandom('id UInt64, t Tuple(m Map(LowCardinality(String), LowCardinality(String)))') LIMIT 100000;

SELECT count(), FROM t_map_lc WHERE NOT ignore(*,  mapKeys(t.m));
SELECT count(), FROM t_map_lc WHERE NOT ignore(*,  t.m.keys);
SELECT count(), FROM t_map_lc WHERE NOT ignore(*,  t.m.values);
SELECT * FROM t_map_lc WHERE mapContains(t.m, 'not_existing_key_1337');

DROP TABLE t_map_lc;

CREATE TABLE t_map_lc
(
    id UInt64,
    t Tuple(m Map(LowCardinality(String), LowCardinality(String)))
)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '10G';

INSERT INTO t_map_lc SELECT * FROM generateRandom('id UInt64, t Tuple(m Map(LowCardinality(String), LowCardinality(String)))') LIMIT 100000;

SELECT count(), FROM t_map_lc WHERE NOT ignore(*,  mapKeys(t.m));
SELECT count(), FROM t_map_lc WHERE NOT ignore(*,  t.m.keys);
SELECT count(), FROM t_map_lc WHERE NOT ignore(*,  t.m.values);
SELECT * FROM t_map_lc WHERE mapContains(t.m, 'not_existing_key_1337');

DROP TABLE t_map_lc;
