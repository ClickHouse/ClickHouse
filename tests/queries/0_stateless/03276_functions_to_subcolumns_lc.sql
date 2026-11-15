DROP TABLE IF EXISTS t_map_lc;

CREATE TABLE t_map_lc
(
    kv Map(LowCardinality(String), LowCardinality(String)),
    k Array(LowCardinality(String)) ALIAS mapKeys(kv),
    v Array(LowCardinality(String)) ALIAS mapValues(kv)
) ENGINE = Memory;

INSERT INTO t_map_lc VALUES (map('foo', 'bar'));

SELECT k, v FROM t_map_lc SETTINGS optimize_functions_to_subcolumns=1;

DROP TABLE t_map_lc;
