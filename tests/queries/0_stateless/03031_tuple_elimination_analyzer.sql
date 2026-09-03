DROP TABLE IF EXISTS test;

SET allow_suspicious_low_cardinality_types = true, enable_analyzer = true;

CREATE TABLE test (`id` LowCardinality(UInt32)) ENGINE = MergeTree ORDER BY id AS SELECT 0;

SELECT tuple(tuple(id) = tuple(1048576)) FROM test;

DROP TABLE test;
