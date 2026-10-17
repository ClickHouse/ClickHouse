-- With map_serialization_version = 'with_key_columns' Map keys are stored sorted.
-- Sorting/partition keys, indices and projections see the sorted Map, while DEFAULT/MATERIALIZED
-- expressions evaluated at write time see the Map in the order it was given.

SET allow_experimental_map_key_columns = 1;

DROP TABLE IF EXISTS t_map_kc_defaults;

CREATE TABLE t_map_kc_defaults
(
    id UInt64,
    m Map(String, UInt64),
    k String MATERIALIZED mapKeys(m)[1],
    d String DEFAULT mapKeys(m)[1],
    a String ALIAS mapKeys(m)[1],
    s String MATERIALIZED arraySort(mapKeys(m))[1]
)
ENGINE = MergeTree ORDER BY (id, mapKeys(m)[1])
SETTINGS map_serialization_version = 'with_key_columns', min_bytes_for_wide_part = 0;

INSERT INTO t_map_kc_defaults (id, m) VALUES (1, {'y':1,'x':2});

SELECT 'insert', m, k, d, a, s FROM t_map_kc_defaults;
SELECT 'pk', count() FROM t_map_kc_defaults WHERE mapKeys(m)[1] = 'x';

ALTER TABLE t_map_kc_defaults MATERIALIZE COLUMN k SETTINGS mutations_sync = 2;
SELECT 'materialize', m, k FROM t_map_kc_defaults;

ALTER TABLE t_map_kc_defaults ADD COLUMN m2 Map(String, UInt64) DEFAULT map('y', 1, 'x', 2), ADD COLUMN k2 String DEFAULT mapKeys(m2)[1];
SELECT 'add column', m2, k2 FROM t_map_kc_defaults;

ALTER TABLE t_map_kc_defaults ADD COLUMN m3 Map(String, UInt64), ADD COLUMN k3 String MATERIALIZED mapKeys(m3)[1];
ALTER TABLE t_map_kc_defaults UPDATE m3 = map('q', 1, 'p', 2) WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'update', m3, k3 FROM t_map_kc_defaults;

DROP TABLE t_map_kc_defaults;
