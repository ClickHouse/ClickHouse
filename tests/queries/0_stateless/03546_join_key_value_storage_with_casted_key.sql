-- Tags: use-rocksdb
-- Old analyzer doesn't support this case
SET enable_analyzer=1;

CREATE TABLE t0 (c0 Bool) ENGINE = EmbeddedRocksDB PRIMARY KEY (c0);
INSERT INTO TABLE t0 (c0) VALUES (TRUE);

SELECT t0.c0 FROM (SELECT NULL c0) v0 RIGHT JOIN t0 USING (c0);
SELECT t0.c0 FROM (SELECT NULL c0) v0 LEFT JOIN t0 USING (c0);
