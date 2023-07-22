SET join_algorithm = 'auto';
SET max_bytes_in_join = 100;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS nr;

CREATE TABLE t (`x` UInt32, `s` LowCardinality(String)) ENGINE = Memory;
CREATE TABLE nr (`x` Nullable(UInt32), `s` Nullable(String)) ENGINE = Memory;

INSERT INTO t VALUES (1, 'l');
INSERT INTO nr VALUES (2, NULL);

SET join_use_nulls = 0;

SELECT t.x, l.s, r.s, toTypeName(l.s), toTypeName(r.s) FROM t AS l LEFT JOIN nr AS r USING (x) ORDER BY t.x;
SELECT t.x, l.s, r.s, toTypeName(l.s), toTypeName(r.s) FROM t AS l RIGHT JOIN nr AS r USING (x) ORDER BY t.x;
SELECT t.x, l.s, r.s, toTypeName(l.s), toTypeName(r.s) FROM t AS l FULL JOIN nr AS r USING (x) ORDER BY t.x;

SELECT '-';

SELECT t.x, l.s, r.s, toTypeName(l.s), toTypeName(r.s) FROM nr AS l LEFT JOIN t AS r USING (x) ORDER BY t.x;
SELECT t.x, l.s, r.s, toTypeName(l.s), toTypeName(r.s) FROM nr AS l RIGHT JOIN t AS r USING (x) ORDER BY t.x;
SELECT t.x, l.s, r.s, toTypeName(l.s), toTypeName(r.s) FROM nr AS l FULL JOIN t AS r USING (x) ORDER BY t.x;

DROP TABLE t;
DROP TABLE nr;
