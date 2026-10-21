-- Test GROUP BY / uniqExact over a QBit column

DROP TABLE IF EXISTS t_qbit_group_by;
CREATE TABLE t_qbit_group_by (id UInt32, vec QBit(Float64, 4)) ENGINE = Memory;
INSERT INTO t_qbit_group_by VALUES (1, [1, 2, 3, 4]), (2, [1, 2, 3, 4]), (3, [5, 6, 7, 8]);

SELECT vec, count() FROM t_qbit_group_by GROUP BY vec ORDER BY toString(vec);
SELECT id % 2 AS k, vec, count() FROM t_qbit_group_by GROUP BY k, vec ORDER BY k, toString(vec);
SELECT uniqExact(vec) FROM t_qbit_group_by;

DROP TABLE t_qbit_group_by;
