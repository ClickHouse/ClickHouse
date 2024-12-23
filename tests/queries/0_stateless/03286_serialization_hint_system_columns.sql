DROP TABLE IF EXISTS t_serialization_hints;

CREATE TABLE t_serialization_hints (a UInt64, b UInt64, c Array(String))
ENGINE = MergeTree ORDER BY a
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_serialization_hints SELECT number, 0, [] FROM numbers(1000);

SELECT name, serialization_hint FROM system.columns
WHERE database = currentDatabase() AND table = 't_serialization_hints'
ORDER BY name;

DROP TABLE t_serialization_hints;
