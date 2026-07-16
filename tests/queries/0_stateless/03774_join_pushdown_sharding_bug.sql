CREATE TABLE IF NOT EXISTS tab_r (`a` UInt32, `b` UInt32, `c` UInt32, `d` UInt32) ENGINE = MergeTree ORDER BY (a * 2, c * 2);
CREATE TABLE IF NOT EXISTS tab_m (`a` UInt32, `b` UInt32, `c` UInt32, `d` UInt32) ENGINE = MergeTree ORDER BY (c + d, b * 2);  
CREATE TABLE IF NOT EXISTS tab_l (`a` UInt32, `b` UInt32, `c` UInt32, `d` UInt32) ENGINE = MergeTree ORDER BY (a * 2, b + c);
INSERT INTO tab_r SELECT number, number, number, number FROM numbers(1000000);
INSERT INTO tab_m SELECT number, number, number, number FROM numbers(1000000);
INSERT INTO tab_l SELECT number, number, number, number FROM numbers(1000000);

SET use_join_disjunctions_push_down = 1;
SET query_plan_join_shard_by_pk_ranges = 1;

SELECT * FROM tab_l AS l 
INNER JOIN tab_m AS m ON l.a = m.a 
INNER JOIN tab_r AS r ON ((l.a * 2) = (r.a * 2)) AND ((l.b + l.c) = (r.c * 2)) AND (l.d = r.d) 
WHERE 0 LIMIT 10;

DROP TABLE IF EXISTS tab_r;
DROP TABLE IF EXISTS tab_m;
DROP TABLE IF EXISTS tab_l;
