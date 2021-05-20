DROP TABLE IF EXISTS view_no_nulls;
DROP TABLE IF EXISTS view_no_nulls_set;
DROP TABLE IF EXISTS view_nulls_set;
DROP TABLE IF EXISTS view_nulls;

SET join_use_nulls = 0;

CREATE OR REPLACE VIEW view_no_nulls AS
SELECT * FROM ( SELECT number + 1 AS a, number + 11 AS b FROM numbers(2) ) AS t1
FULL JOIN ( SELECT number + 2 AS a, number + 22 AS c FROM numbers(2) ) AS t2
USING a ORDER BY a
SETTINGS max_block_size = 666;

-- check that max_block_size not rewriten
EXPLAIN SYNTAX SELECT * FROM view_no_nulls;

CREATE OR REPLACE VIEW view_nulls_set AS
SELECT * FROM ( SELECT number + 1 AS a, number + 11 AS b FROM numbers(2) ) AS t1
FULL JOIN ( SELECT number + 2 AS a, number + 22 AS c FROM numbers(2) ) AS t2
USING a ORDER BY a
SETTINGS join_use_nulls = 1, max_block_size = 666;

SET join_use_nulls = 1;

CREATE OR REPLACE VIEW view_nulls AS
SELECT * FROM ( SELECT number + 1 AS a, number + 11 AS b FROM numbers(2) ) AS t1
FULL JOIN ( SELECT number + 2 AS a, number + 22 AS c FROM numbers(2) ) AS t2
USING a ORDER BY a;

CREATE OR REPLACE VIEW view_no_nulls_set AS
SELECT * FROM ( SELECT number + 1 AS a, number + 11 AS b FROM numbers(2) ) AS t1
FULL JOIN ( SELECT number + 2 AS a, number + 22 AS c FROM numbers(2) ) AS t2
USING a ORDER BY a
SETTINGS join_use_nulls = 0;

SET join_use_nulls = 1;

SELECT * from view_no_nulls;
SELECT * from view_no_nulls_set;
SELECT * from view_nulls_set;
SELECT * from view_nulls;

SET join_use_nulls = 0;

SELECT * from view_no_nulls;
SELECT * from view_no_nulls_set;
SELECT * from view_nulls_set;
SELECT * from view_nulls;

DROP TABLE IF EXISTS view_no_nulls;
DROP TABLE IF EXISTS view_no_nulls_set;
DROP TABLE IF EXISTS view_nulls_set;
DROP TABLE IF EXISTS view_nulls;
