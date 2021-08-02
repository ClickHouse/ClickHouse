DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS nl;
DROP TABLE IF EXISTS nr;
DROP TABLE IF EXISTS l_lc;
DROP TABLE IF EXISTS r_lc;

CREATE TABLE l (x UInt32, lc String) ENGINE = Memory;
CREATE TABLE r (x UInt32, lc String) ENGINE = Memory;
CREATE TABLE nl (x Nullable(UInt32), lc Nullable(String)) ENGINE = Memory;
CREATE TABLE nr (x Nullable(UInt32), lc Nullable(String)) ENGINE = Memory;
CREATE TABLE l_lc (x UInt32, lc LowCardinality(String)) ENGINE = Memory;
CREATE TABLE r_lc (x UInt32, lc LowCardinality(String)) ENGINE = Memory;

INSERT INTO r VALUES (0, 'str');
INSERT INTO nr VALUES (0, 'str');
INSERT INTO r_lc VALUES (0, 'str');

--

SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x);
SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc);
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x);
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc);
 
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(r.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(r.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(r.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(r.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc);

--

SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r USING (x);
SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r USING (lc);
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r USING (x);
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r USING (lc);

SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM l_lc AS l RIGHT JOIN r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM l_lc AS l RIGHT JOIN r USING (lc);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM l_lc AS l FULL JOIN r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM l_lc AS l FULL JOIN r USING (lc);

--

SELECT lc, toTypeName(lc) FROM l RIGHT JOIN r USING (x);
SELECT lc, toTypeName(lc) FROM l RIGHT JOIN r USING (lc);
SELECT lc, toTypeName(lc) FROM l FULL JOIN r USING (x);
SELECT lc, toTypeName(lc) FROM l FULL JOIN r USING (lc);

SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM l RIGHT JOIN r_lc AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM l RIGHT JOIN r_lc AS r USING (lc);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM l FULL JOIN r_lc AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM l FULL JOIN r_lc AS r USING (lc);

--

SELECT lc, toTypeName(lc) FROM l_lc RIGHT JOIN nr USING (x);
SELECT lc, toTypeName(lc) FROM l_lc RIGHT JOIN nr USING (lc);
SELECT lc, toTypeName(lc) FROM l_lc FULL JOIN nr USING (x);
SELECT lc, toTypeName(lc) FROM l_lc FULL JOIN nr USING (lc);

SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM l_lc AS l RIGHT JOIN nr AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM l_lc AS l RIGHT JOIN nr AS r USING (lc);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM l_lc AS l FULL JOIN nr AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM l_lc AS l FULL JOIN nr AS r USING (lc);

--

SELECT lc, toTypeName(lc) FROM nl RIGHT JOIN r_lc USING (x);
SELECT lc, toTypeName(lc) FROM nl RIGHT JOIN r_lc USING (lc);
SELECT lc, toTypeName(lc) FROM nl FULL JOIN r_lc USING (x);
SELECT lc, toTypeName(lc) FROM nl FULL JOIN r_lc USING (lc);

SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM nl AS l RIGHT JOIN r_lc AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM nl AS l RIGHT JOIN r_lc AS r USING (lc);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM nl AS l FULL JOIN r_lc AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc), toTypeName(materialize(r.lc)) FROM nl AS l FULL JOIN r_lc AS r USING (lc);

-- TODO: LC nullability
SET join_use_nulls = 1;

SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x);
SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc);
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x);
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc);

SELECT l.lc, r.lc, toTypeName(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc);

--

SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r USING (x);
SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r USING (lc);
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r USING (x);
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r USING (lc);

SELECT l.lc, r.lc, toTypeName(l.lc) FROM l_lc AS l RIGHT JOIN r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM l_lc AS l RIGHT JOIN r USING (lc);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM l_lc AS l FULL JOIN r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM l_lc AS l FULL JOIN r USING (lc);

--

SELECT lc, toTypeName(lc) FROM l RIGHT JOIN r USING (x);
SELECT lc, toTypeName(lc) FROM l RIGHT JOIN r USING (lc);
SELECT lc, toTypeName(lc) FROM l FULL JOIN r USING (x);
SELECT lc, toTypeName(lc) FROM l FULL JOIN r USING (lc);

SELECT l.lc, r.lc, toTypeName(l.lc) FROM l RIGHT JOIN r_lc AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM l RIGHT JOIN r_lc AS r USING (lc);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM l FULL JOIN r_lc AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM l FULL JOIN r_lc AS r USING (lc);

--

SELECT lc, toTypeName(lc) FROM l_lc RIGHT JOIN nr USING (x);
SELECT lc, toTypeName(lc) FROM l_lc RIGHT JOIN nr USING (lc);
SELECT lc, toTypeName(lc) FROM l_lc FULL JOIN nr USING (x);
SELECT lc, toTypeName(lc) FROM l_lc FULL JOIN nr USING (lc);

SELECT l.lc, r.lc, toTypeName(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (lc);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (lc);

--

SELECT lc, toTypeName(lc) FROM nl RIGHT JOIN r_lc USING (x);
SELECT lc, toTypeName(lc) FROM nl RIGHT JOIN r_lc USING (lc);
SELECT lc, toTypeName(lc) FROM nl FULL JOIN r_lc USING (x);
SELECT lc, toTypeName(lc) FROM nl FULL JOIN r_lc USING (lc);

SELECT l.lc, r.lc, toTypeName(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (lc);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (x);
SELECT l.lc, r.lc, toTypeName(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (lc);

DROP TABLE l;
DROP TABLE r;
DROP TABLE nl;
DROP TABLE nr;
DROP TABLE l_lc;
DROP TABLE r_lc;
