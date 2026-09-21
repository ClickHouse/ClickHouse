-- Function predicates like toYear(d) = 1500 on an ORDER BY d table must be rewritten to a raw range
-- over the extended Date32 range [0000-01-01, 9999-12-31], so the primary key can prune.

SET optimize_time_filter_with_preimage = 1;

DROP TABLE IF EXISTS date32_preimage;
CREATE TABLE date32_preimage (d Date32) ENGINE = MergeTree ORDER BY d;
INSERT INTO date32_preimage VALUES ('0000-01-01'), ('1500-06-15'), ('1899-12-31'), ('1970-01-01'), ('2299-12-31'), ('2300-01-01'), ('9000-12-31'), ('9999-12-31');

SELECT 'toYear below the LUT range is rewritten to a key range';
EXPLAIN QUERY TREE run_passes=1 SELECT count() FROM date32_preimage WHERE toYear(d) = 1500 SETTINGS enable_analyzer=1;
SELECT count() FROM date32_preimage WHERE toYear(d) = 1500;

SELECT 'toYear above the LUT range is rewritten to a key range';
EXPLAIN QUERY TREE run_passes=1 SELECT count() FROM date32_preimage WHERE toYear(d) = 9000 SETTINGS enable_analyzer=1;
SELECT count() FROM date32_preimage WHERE toYear(d) = 9000;

SELECT 'toYYYYMM outside the LUT range is rewritten to a key range';
EXPLAIN QUERY TREE run_passes=1 SELECT count() FROM date32_preimage WHERE toYYYYMM(d) = 900012 SETTINGS enable_analyzer=1;
SELECT count() FROM date32_preimage WHERE toYYYYMM(d) = 900012;
EXPLAIN QUERY TREE run_passes=1 SELECT count() FROM date32_preimage WHERE toYYYYMM(d) = 150006 SETTINGS enable_analyzer=1;
SELECT count() FROM date32_preimage WHERE toYYYYMM(d) = 150006;

SELECT 'inequalities over the extended range';
SELECT count() FROM date32_preimage WHERE toYear(d) < 1900;
SELECT count() FROM date32_preimage WHERE toYear(d) > 2299;
SELECT count() FROM date32_preimage WHERE toYYYYMM(d) >= 230001;

SELECT 'the boundary year 9999 has no representable exclusive upper endpoint, so it is not rewritten, but the result is correct';
SELECT count() FROM date32_preimage WHERE toYear(d) = 9999;
SELECT count() FROM date32_preimage WHERE toYYYYMM(d) = 999912;

SELECT 'year 0 is rewritten and correct';
EXPLAIN QUERY TREE run_passes=1 SELECT count() FROM date32_preimage WHERE toYear(d) = 0 SETTINGS enable_analyzer=1;
SELECT count() FROM date32_preimage WHERE toYear(d) = 0;

DROP TABLE date32_preimage;
