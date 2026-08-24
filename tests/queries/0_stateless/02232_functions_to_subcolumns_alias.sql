DROP TABLE IF EXISTS t_functions_to_subcolumns_alias;

CREATE TABLE t_functions_to_subcolumns_alias (id UInt64, t Tuple(UInt64, String), m Map(String, UInt64)) ENGINE = Memory;
INSERT INTO t_functions_to_subcolumns_alias VALUES (1, (100, 'abc'), map('foo', 1, 'bar', 2)) (2, NULL, map());

SELECT count(id) AS cnt FROM t_functions_to_subcolumns_alias FORMAT TSVWithNames;
SELECT tupleElement(t, 1) as t0, t0 FROM t_functions_to_subcolumns_alias FORMAT TSVWithNames;
SELECT mapContains(m, 'foo') AS hit FROM t_functions_to_subcolumns_alias FORMAT TSVWithNames;

DROP TABLE t_functions_to_subcolumns_alias;
