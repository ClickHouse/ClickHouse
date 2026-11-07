-- `ALP` is generally available: it is accepted with the former gate setting disabled.
SET enable_alp_codec = 0;

DROP TABLE IF EXISTS t_alp_ga;

CREATE TABLE t_alp_ga (x Float64 CODEC(ALP)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alp_ga VALUES (1.5), (2.5);
ALTER TABLE t_alp_ga ADD COLUMN y Float32 DEFAULT x CODEC(ALP, ZSTD);
ALTER TABLE t_alp_ga MODIFY COLUMN y Float32 CODEC(ALP);
SELECT sum(x), sum(y) FROM t_alp_ga;
DROP TABLE t_alp_ga;

SELECT tier FROM system.codecs WHERE name = 'ALP';
SELECT tier FROM system.settings WHERE name = 'enable_alp_codec';
