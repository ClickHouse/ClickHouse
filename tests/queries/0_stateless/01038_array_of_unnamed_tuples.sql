SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS array_of_tuples;

CREATE TABLE array_of_tuples 
(
    f Array(Tuple(Float64, Float64)), 
    s Array(Tuple(UInt8, UInt16, UInt32))
) ENGINE = Memory;

INSERT INTO array_of_tuples values ([(1, 2), (2, 3), (3, 4)], array(tuple(1, 2, 3), tuple(2, 3, 4))), (array((1.0, 2.0)), [tuple(4, 3, 1)]);

SELECT f from array_of_tuples;
SELECT s from array_of_tuples;

DROP TABLE array_of_tuples;
