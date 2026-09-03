SET allow_suspicious_codecs = 1;
CREATE TABLE t0 (c0 Float64 CODEC(ZSTD, DoubleDelta)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE t0 (c0) VALUES (NULL), (1);
SELECT c0 FROM t0;
