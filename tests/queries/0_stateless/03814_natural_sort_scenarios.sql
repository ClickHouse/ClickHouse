SELECT concat('x', toString(number)) AS s FROM numbers(15) ORDER BY s NATURAL;

DROP TABLE IF EXISTS t_natural_nullable;
CREATE TABLE t_natural_nullable(k Nullable(String)) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO t_natural_nullable VALUES ('item2'), ('item10'), (NULL), ('item1'), ('item0');
SELECT * FROM t_natural_nullable ORDER BY k NATURAL NULLS LAST;
DROP TABLE t_natural_nullable;


DROP TABLE IF EXISTS t_natural_lc;
CREATE TABLE t_natural_lc(k LowCardinality(String)) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO t_natural_lc SELECT concat('file', toString(number)) FROM numbers(12);
SELECT * FROM t_natural_lc ORDER BY k NATURAL;
DROP TABLE t_natural_lc;


DROP TABLE IF EXISTS t_natural_multi;
CREATE TABLE t_natural_multi(a String, b Int32) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO t_natural_multi VALUES ('item2', 1), ('item10', 2), ('item1', 3), ('item10', 0);
SELECT * FROM t_natural_multi ORDER BY a NATURAL, b;
DROP TABLE t_natural_multi;

SELECT s FROM (SELECT concat('ver', toString(number)) AS s FROM numbers(6)) ORDER BY s DESC NATURAL;

DROP TABLE IF EXISTS t_natural_fixed;
CREATE TABLE t_natural_fixed(k FixedString(8)) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO t_natural_fixed VALUES ('item0001'), ('item0010'), ('item0002');
SELECT * FROM t_natural_fixed ORDER BY k NATURAL;
DROP TABLE t_natural_fixed;
