DROP TABLE IF EXISTS t;
CREATE TABLE t (`col` Tuple(Int32, Int32)) ENGINE = Memory;
INSERT INTO t VALUES ((1, 2)), ((3, 4));

SELECT untuple(arrayJoin((SELECT tupleToNameValuePairs(col)))) FROM t
FORMAT Null;

SELECT
    tupleElement(arrayJoin((SELECT tupleToNameValuePairs(col))), '1'),
    tupleElement(arrayJoin((SELECT tupleToNameValuePairs(col))), '2')
FROM t
FORMAT Null;
