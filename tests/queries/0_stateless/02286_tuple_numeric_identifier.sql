-- Tags: no-fasttest

DROP TABLE IF EXISTS t_tuple_numeric;

CREATE TABLE t_tuple_numeric (t Tuple(`1` Tuple(`2` Int, `3` Int), `4` Int)) ENGINE = Memory;
SHOW CREATE TABLE t_tuple_numeric;

INSERT INTO t_tuple_numeric VALUES (((2, 3), 4));

SET output_format_json_named_tuples_as_objects = 1;

SELECT * FROM t_tuple_numeric FORMAT JSONEachRow;
SELECT `t`.`1`.`2`, `t`.`1`.`3`, `t`.`4` FROM t_tuple_numeric;
SELECT t.1.1, t.1.2, t.2 FROM t_tuple_numeric;

SELECT t.1.3 FROM t_tuple_numeric; -- {serverError ILLEGAL_INDEX}
SELECT t.4 FROM t_tuple_numeric; -- {serverError ILLEGAL_INDEX}
SELECT `t`.`1`.`1`, `t`.`1`.`2`, `t`.`2` FROM t_tuple_numeric; -- {serverError UNKNOWN_IDENTIFIER}

DROP TABLE t_tuple_numeric;

CREATE TABLE t_tuple_numeric (t Tuple(Tuple(Int, Int), Int)) ENGINE = Memory;

INSERT INTO t_tuple_numeric VALUES (((2, 3), 4));

SELECT t.1.1, t.1.2, t.2 FROM t_tuple_numeric;
SELECT `t`.`1`.`1`, `t`.`1`.`2`, `t`.`2` FROM t_tuple_numeric;

DROP TABLE t_tuple_numeric;

SET allow_experimental_object_type = 1;
CREATE TABLE t_tuple_numeric (t JSON) ENGINE = Memory;
INSERT INTO t_tuple_numeric FORMAT JSONEachRow {"t":{"1":{"2":2,"3":3},"4":4}}

SELECT toTypeName(t) FROM t_tuple_numeric LIMIT 1;

SELECT * FROM t_tuple_numeric FORMAT JSONEachRow;
SELECT `t`.`1`.`2`, `t`.`1`.`3`, `t`.`4` FROM t_tuple_numeric;

DROP TABLE t_tuple_numeric;

WITH
    '{"1":{"key":"value"}}' AS data,
    JSONExtract(data, 'Tuple("1" Tuple(key String))') AS parsed_json
SELECT parsed_json AS ssid
