set max_threads=1;
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Variant(String, Int)) ENGINE = MergeTree() PRIMARY KEY tuple() SETTINGS use_compact_variant_discriminators_serialization = 0, index_granularity=1;
INSERT INTO TABLE t0 (c0) VALUES (42), ('a');
optimize table t0 final;
SELECT c0 FROM t0;
DROP TABLE t0;

