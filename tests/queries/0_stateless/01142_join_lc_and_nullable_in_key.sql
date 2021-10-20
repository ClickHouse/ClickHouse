DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS nr;

CREATE TABLE t (`x` UInt32, `lc` LowCardinality(String)) ENGINE = Memory;
CREATE TABLE nr (`x` Nullable(UInt32), `lc` Nullable(String)) ENGINE = Memory;

INSERT INTO t VALUES (1, 'l');
INSERT INTO nr VALUES (2, NULL);

SET join_use_nulls = 0;

SELECT x, lc, r.lc, toTypeName(r.lc) FROM t AS l LEFT JOIN nr AS r USING (x) ORDER BY x;
SELECT x, lc, r.lc, toTypeName(r.lc) FROM t AS l RIGHT JOIN nr AS r USING (x) ORDER BY x;
SELECT x, lc, r.lc, toTypeName(r.lc) FROM t AS l FULL JOIN nr AS r USING (x) ORDER BY x;

SELECT '-';

SELECT x, lc, r.lc, toTypeName(r.lc) FROM t AS l LEFT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, r.lc, toTypeName(r.lc) FROM t AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, r.lc, toTypeName(r.lc) FROM t AS l FULL JOIN nr AS r USING (lc) ORDER BY x;

SELECT '-';

SELECT x, lc, materialize(r.lc) y, toTypeName(y) FROM t AS l LEFT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, materialize(r.lc) y, toTypeName(y) FROM t AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, materialize(r.lc) y, toTypeName(y) FROM t AS l FULL JOIN nr AS r USING (lc) ORDER BY x;

SELECT '-';

SET join_use_nulls = 1;

SELECT x, lc, r.lc, toTypeName(r.lc) FROM t AS l LEFT JOIN nr AS r USING (x) ORDER BY x;
SELECT x, lc, r.lc, toTypeName(r.lc) FROM t AS l RIGHT JOIN nr AS r USING (x) ORDER BY x;
SELECT x, lc, r.lc, toTypeName(r.lc) FROM t AS l FULL JOIN nr AS r USING (x) ORDER BY x;

SELECT '-';

SELECT x, lc, r.lc, toTypeName(r.lc) FROM t AS l LEFT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, r.lc, toTypeName(r.lc) FROM t AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, r.lc, toTypeName(r.lc) FROM t AS l FULL JOIN nr AS r USING (lc) ORDER BY x;

SELECT '-';

SELECT x, lc, materialize(r.lc) y, toTypeName(y) FROM t AS l LEFT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, materialize(r.lc) y, toTypeName(y) FROM t AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, materialize(r.lc) y, toTypeName(y) FROM t AS l FULL JOIN nr AS r USING (lc) ORDER BY x;


DROP TABLE t;
DROP TABLE nr;
