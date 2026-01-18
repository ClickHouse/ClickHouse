SET enable_analyzer = 1;
SET allow_suspicious_primary_key = 1;

CREATE TABLE t0 (c0 Nullable(Int)) ENGINE = SummingMergeTree() ORDER BY tuple() PARTITION BY (c0) SETTINGS allow_nullable_key = 1;
INSERT INTO TABLE t0 (c0) VALUES (NULL);
SELECT * FROM t0 FINAL;
SELECT count() FROM t0 FINAL WHERE ((t0.c0 IS NULL) = TRUE);
