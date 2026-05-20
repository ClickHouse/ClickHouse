DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS dim;
DROP TABLE IF EXISTS rt;
DROP TABLE IF EXISTS lk1;
DROP TABLE IF EXISTS lk2;
DROP TABLE IF EXISTS lk3;

CREATE TABLE fact (id UInt32, grp String, fy String, cc String, x2 String) ENGINE = MergeTree() ORDER BY (grp, fy);
CREATE TABLE dim  (grp String, fy String, cc String, dt String, cl String) ENGINE = MergeTree() ORDER BY (grp, fy);
CREATE TABLE rt   (jk String, cc String, dt String, cl String) ENGINE = MergeTree() ORDER BY (jk, cc);
CREATE TABLE lk1  (dt String, d String, cl String) ENGINE = MergeTree() ORDER BY dt;
CREATE TABLE lk2  (cc String, d String, cl String) ENGINE = MergeTree() ORDER BY cc;
CREATE TABLE lk3  (x2 String, d String) ENGINE = MergeTree() ORDER BY x2;

INSERT INTO fact SELECT number+1, toString(intDiv(number,5)+1), '2025', toString((intDiv(number,5)%10)+1), toString((number%20)+1) FROM numbers(36000);

INSERT INTO dim  SELECT toString(number+1), '2025', toString((number%10)+1), toString((number%5)+1), '100' FROM numbers(7200);
INSERT INTO rt   SELECT concat(toString(number+1),'2025'), toString((number%10)+1), toString((number%5)+1), '100' FROM numbers(7200);
INSERT INTO lk1  SELECT toString(number+1), 'l1', '100' FROM numbers(5);
INSERT INTO lk2  SELECT toString(number+1), 'l2', '100' FROM numbers(10);
INSERT INTO lk3  SELECT toString(number+1), 'l3' FROM numbers(20);

SELECT count(), uniqExact(r_id), if(uniqExact(r_id) = 4, 'PASS', 'FAIL')
  FROM (SELECT number AS l_key FROM numbers(2)) AS l
  RIGHT ANY JOIN (SELECT intDiv(number, 2) AS r_key, number AS r_id FROM numbers(4)) AS r
  ON l.l_key = r.r_key
SETTINGS max_joined_block_size_rows = 2, join_algorithm = 'hash';

SET query_plan_join_swap_table = 0;
SET join_algorithm = 'hash';

SELECT count(), uniqExact(id), if(uniqExact(id) = 36000, 'PASS', 'FAIL') AS result
FROM rt AS rt
RIGHT ANY JOIN (
    SELECT fact.id AS id, fact.x2 AS x2, dim.cc AS cc, dim.cl AS cl, dim.grp AS grp, dim.fy AS fy
    FROM fact AS fact
    INNER JOIN dim AS dim ON dim.grp = fact.grp AND dim.fy = fact.fy
) AS fd ON rt.jk = concat(fd.grp, fd.fy) AND rt.cc = fd.cc
LEFT JOIN lk1 AS l1 ON l1.dt = rt.dt AND l1.cl = rt.cl
LEFT JOIN lk2 AS l2 ON l2.cc = fd.cc AND l2.cl = fd.cl
LEFT JOIN lk3 AS l3 ON l3.x2 = fd.x2
;
