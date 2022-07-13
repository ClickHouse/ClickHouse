DROP TABLE IF EXISTS t_tuple_element_default;

CREATE TABLE t_tuple_element_default(t1 Tuple(a UInt32, s String), t2 Tuple(UInt32, String)) ENGINE = Memory;
INSERT INTO t_tuple_element_default VALUES ((1, 'a'), (2, 'b'));

SELECT tupleElement(t1, 'z', 'z') FROM t_tuple_element_default;
EXPLAIN SYNTAX SELECT tupleElement(t1, 'z', 'z') FROM t_tuple_element_default;
SELECT tupleElement(t1, 'z', 0) FROM t_tuple_element_default;
EXPLAIN SYNTAX SELECT tupleElement(t1, 'z', 0) FROM t_tuple_element_default;
SELECT tupleElement(t2, 'z', 'z') FROM t_tuple_element_default;
EXPLAIN SYNTAX SELECT tupleElement(t2, 'z', 'z') FROM t_tuple_element_default;

SELECT tupleElement(t1, 3, 'z') FROM t_tuple_element_default; -- { serverError 127 }
SELECT tupleElement(t1, 0, 'z') FROM t_tuple_element_default; -- { serverError 127 }

DROP TABLE t_tuple_element_default;

SELECT tupleElement(array(tuple(1, 2)), 'a', 0); -- { serverError 645 }
SELECT tupleElement(array(tuple(1, 2)), 'a', array(tuple(1, 2), tuple(3, 4))); -- { serverError 190 }

SELECT tupleElement(array(tuple(1, 2)), 'a', array(tuple(3, 4)));
EXPLAIN SYNTAX SELECT tupleElement(array(tuple(1, 2)), 'a', array(tuple(3, 4)));

SELECT tupleElement(array(array(tuple(1))), 'a', array(array(1, 2, 3)));
EXPLAIN SYNTAX SELECT tupleElement(array(array(tuple(1))), 'a', array(array(1, 2, 3)));

CREATE TABLE t_tuple_element_default(t1 Array(Tuple(UInt32)), t2 UInt32) ENGINE = Memory;

SELECT tupleElement(t1, 'a', array(tuple(1))) FROM t_tuple_element_default;
EXPLAIN SYNTAX SELECT tupleElement(t1, 'a', array(tuple(1))) FROM t_tuple_element_default;

INSERT INTO t_tuple_element_default VALUES ([(1)], 100);

SELECT tupleElement(t1, 'a', array(tuple(0))) FROM t_tuple_element_default;
EXPLAIN SYNTAX SELECT tupleElement(t1, 'a', array(tuple(0))) FROM t_tuple_element_default;

INSERT INTO t_tuple_element_default VALUES ([(2), (3), (4)], 234);

SELECT tupleElement(t1, 'a', array(tuple(0))) FROM t_tuple_element_default; -- { serverError 190 }
EXPLAIN SYNTAX SELECT tupleElement(t1, 'a', array(tuple(0))) FROM t_tuple_element_default;

DROP TABLE t_tuple_element_default;

