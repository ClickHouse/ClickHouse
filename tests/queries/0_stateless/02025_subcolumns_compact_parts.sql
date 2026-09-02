DROP TABLE IF EXISTS t_comp_subcolumns;

CREATE TABLE t_comp_subcolumns (id UInt32, n Nullable(String), arr Array(Array(UInt32)))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_comp_subcolumns SELECT number, 'a', [range(number % 11), range(number % 13)] FROM numbers(20000);

SELECT sum(n.null) FROM t_comp_subcolumns;
SELECT n.null FROM t_comp_subcolumns LIMIT 10000, 5;

SELECT sum(arr.size0) FROM t_comp_subcolumns;
SELECT sumArray(arr.size1) FROM t_comp_subcolumns;

DROP TABLE t_comp_subcolumns;
