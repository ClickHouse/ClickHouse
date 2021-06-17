DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS nl;
DROP TABLE IF EXISTS nr;
DROP TABLE IF EXISTS l_lc;
DROP TABLE IF EXISTS r_lc;

CREATE TABLE l (x UInt32, lc String) ENGINE = TinyLog;
CREATE TABLE r (x UInt32, lc String) ENGINE = TinyLog;
CREATE TABLE nl (x Nullable(UInt32), lc Nullable(String)) ENGINE = TinyLog;
CREATE TABLE nr (x Nullable(UInt32), lc Nullable(String)) ENGINE = TinyLog;
CREATE TABLE l_lc (x UInt32, lc LowCardinality(String)) ENGINE = TinyLog;
CREATE TABLE r_lc (x UInt32, lc LowCardinality(String)) ENGINE = TinyLog;

INSERT INTO r VALUES (0, 'str'),  (1, 'str_r');
INSERT INTO nr VALUES (0, 'str'),  (1, 'str_r');
INSERT INTO r_lc VALUES (0, 'str'),  (1, 'str_r');

INSERT INTO l VALUES (0, 'str'), (2, 'str_l');
INSERT INTO nl VALUES (0, 'str'), (2, 'str_l');
INSERT INTO l_lc VALUES (0, 'str'), (2, 'str_l');

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

SET join_use_nulls = 0;

SELECT lc, toTypeName(lc)  FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY l.lc;

DROP TABLE l;
DROP TABLE r;
DROP TABLE nl;
DROP TABLE nr;
DROP TABLE l_lc;
DROP TABLE r_lc;
