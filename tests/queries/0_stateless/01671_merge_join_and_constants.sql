SET output_format_pretty_color=1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;

CREATE TABLE table1(a String, b Date) ENGINE MergeTree order by a;
CREATE TABLE table2(c String, a String, d Date) ENGINE MergeTree order by c;

INSERT INTO table1 VALUES ('a', '2018-01-01') ('b', '2018-01-01') ('c', '2018-01-01');
INSERT INTO table2 VALUES ('D', 'd', '2018-01-01') ('B', 'b', '2018-01-01') ('C', 'c', '2018-01-01');

set join_algorithm = 'partial_merge';

SELECT * FROM table1 AS t1 ALL LEFT JOIN (SELECT *, '0.10', c, d AS b FROM table2) AS t2 USING (a, b) ORDER BY d, t1.a ASC FORMAT PrettyCompact settings max_rows_in_join = 1;

SELECT pow('0.0000000257', NULL), pow(pow(NULL, NULL), NULL) - NULL, (val + NULL) = (rval * 0), * FROM (SELECT (val + 256) = (NULL * NULL), toLowCardinality(toNullable(dummy)) AS val FROM system.one) AS s1 ANY LEFT JOIN (SELECT toLowCardinality(dummy) AS rval FROM system.one) AS s2 ON (val + 0) = (rval * 255) settings max_rows_in_join = 1;

DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;
