DROP TABLE IF EXISTS join_tbl;

CREATE TABLE join_tbl (`id` String, `name` String, lcname LowCardinality(String)) ENGINE = Join(any, left, id);

INSERT INTO join_tbl VALUES ('xxx', 'yyy', 'yyy');

SELECT joinGet('join_tbl', 'name', 'xxx') == 'yyy';
SELECT joinGet('join_tbl', 'name', toLowCardinality('xxx')) == 'yyy';
SELECT joinGet('join_tbl', 'name', toLowCardinality(materialize('xxx'))) == 'yyy';
SELECT joinGet('join_tbl', 'lcname', 'xxx') == 'yyy';
SELECT joinGet('join_tbl', 'lcname', toLowCardinality('xxx')) == 'yyy';
SELECT joinGet('join_tbl', 'lcname', toLowCardinality(materialize('xxx'))) == 'yyy';

DROP TABLE IF EXISTS join_tbl;
