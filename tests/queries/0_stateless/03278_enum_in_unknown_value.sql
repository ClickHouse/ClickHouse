DROP TABLE IF EXISTS t_enum_in_unknown_value;

CREATE TABLE t_enum_in_unknown_value (e Enum('a'=1, 'b'=2)) ENGINE=Memory;

INSERT INTO t_enum_in_unknown_value VALUES ('a');

SELECT * FROM t_enum_in_unknown_value;

SELECT * FROM t_enum_in_unknown_value WHERE e IN ('a');
SELECT * FROM t_enum_in_unknown_value WHERE e NOT IN ('a');

SELECT * FROM t_enum_in_unknown_value WHERE e IN ('a', 'b');
SELECT * FROM t_enum_in_unknown_value WHERE e NOT IN ('a', 'b');

SELECT * FROM t_enum_in_unknown_value WHERE e IN (1, 'b');
SELECT * FROM t_enum_in_unknown_value WHERE e NOT IN (1, 'b');

SELECT * FROM t_enum_in_unknown_value WHERE e IN ('a', 'c');
SELECT * FROM t_enum_in_unknown_value WHERE e NOT IN ('a', 'c');

SELECT * FROM t_enum_in_unknown_value WHERE e IN ('a', 'b', 'c');
SELECT * FROM t_enum_in_unknown_value WHERE e NOT IN ('a', 'b', 'c');

SELECT * FROM t_enum_in_unknown_value WHERE e IN ('c');
SELECT * FROM t_enum_in_unknown_value WHERE e NOT IN ('c');

SET validate_enum_literals_in_operators = 1;

SELECT * FROM t_enum_in_unknown_value WHERE e IN ('a', 'b', 'c'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT * FROM t_enum_in_unknown_value WHERE e NOT IN ('a', 'b', 'c'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT * FROM t_enum_in_unknown_value WHERE e IN ('a', 'b', 3); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
