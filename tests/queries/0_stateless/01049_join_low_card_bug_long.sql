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

INSERT INTO r VALUES (0, 'str'),  (1, 'str_r');
INSERT INTO nr VALUES (0, 'str'),  (1, 'str_r');
INSERT INTO r_lc VALUES (0, 'str'),  (1, 'str_r');

INSERT INTO l VALUES (0, 'str'), (2, 'str_l');
INSERT INTO nl VALUES (0, 'str'), (2, 'str_l');
INSERT INTO l_lc VALUES (0, 'str'), (2, 'str_l');


SET join_use_nulls = 0;

--

SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM l RIGHT JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l RIGHT JOIN r USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l FULL JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l FULL JOIN r USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM l_lc RIGHT JOIN nr USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc RIGHT JOIN nr USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc FULL JOIN nr USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc FULL JOIN nr USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM nl RIGHT JOIN r_lc USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM nl RIGHT JOIN r_lc USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM nl FULL JOIN r_lc USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM nl FULL JOIN r_lc USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

SELECT '-- join_use_nulls --';

SET join_use_nulls = 1;

SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM l RIGHT JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l RIGHT JOIN r USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l FULL JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l FULL JOIN r USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM l_lc RIGHT JOIN nr USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc RIGHT JOIN nr USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc FULL JOIN nr USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc FULL JOIN nr USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM nl RIGHT JOIN r_lc USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM nl RIGHT JOIN r_lc USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM nl FULL JOIN r_lc USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM nl FULL JOIN r_lc USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

set join_algorithm = 'partial_merge';

SET join_use_nulls = 0;

--

SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM l RIGHT JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l RIGHT JOIN r USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l FULL JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l FULL JOIN r USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM l_lc RIGHT JOIN nr USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc RIGHT JOIN nr USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc FULL JOIN nr USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc FULL JOIN nr USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM nl RIGHT JOIN r_lc USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM nl RIGHT JOIN r_lc USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM nl FULL JOIN r_lc USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM nl FULL JOIN r_lc USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

SELECT '-- join_use_nulls --';

SET join_use_nulls = 1;

SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM l RIGHT JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l RIGHT JOIN r USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l FULL JOIN r USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l FULL JOIN r USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM l_lc RIGHT JOIN nr USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc RIGHT JOIN nr USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc FULL JOIN nr USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM l_lc FULL JOIN nr USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (lc) ORDER BY x;

--

SELECT lc, toTypeName(lc) FROM nl RIGHT JOIN r_lc USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM nl RIGHT JOIN r_lc USING (lc) ORDER BY x;
SELECT lc, toTypeName(lc) FROM nl FULL JOIN r_lc USING (x) ORDER BY x;
SELECT lc, toTypeName(lc) FROM nl FULL JOIN r_lc USING (lc) ORDER BY x;

SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (x) ORDER BY x;
SELECT toTypeName(r.lc), toTypeName(materialize(r.lc)), r.lc, materialize(r.lc), toTypeName(l.lc), toTypeName(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x;

SET join_use_nulls = 0;
SELECT lc, toTypeName(lc)  FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY l.lc;

SELECT lowCardinalityKeys(lc.lc) FROM r FULL JOIN l_lc as lc USING (lc) ORDER BY lowCardinalityKeys(lc.lc);

DROP TABLE l;
DROP TABLE r;
DROP TABLE nl;
DROP TABLE nr;
DROP TABLE l_lc;
DROP TABLE r_lc;
