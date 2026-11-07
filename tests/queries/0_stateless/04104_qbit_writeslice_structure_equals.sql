-- Regression test for PR #103084 - INSERT into Tuple(Map(..., QBit)) used to
-- raise a logical error in writeSlice because ColumnQBit::structureEquals always
-- returned false for two ColumnQBit columns. Direct coverage of structureEquals
-- (including the negative dimension-mismatch case) lives in gtest_column_qbit.cpp.

DROP TABLE IF EXISTS t_qbit_ws;
CREATE TABLE t_qbit_ws (c0 Tuple(Map(String, Nullable(QBit(Float64, 4))))) ENGINE = Memory;
INSERT INTO t_qbit_ws SELECT NULL;
SELECT * FROM t_qbit_ws;
DROP TABLE t_qbit_ws;

DROP TABLE IF EXISTS t_qbit_ws_full;
CREATE TABLE t_qbit_ws_full (
    c0 Tuple(Nullable(Int8), Enum16('a' = 1, 'b' = 2), Map(MultiLineString, Nullable(QBit(Float64, 4))))
) ENGINE = Memory;
INSERT INTO t_qbit_ws_full SELECT NULL;
SELECT * FROM t_qbit_ws_full;
DROP TABLE t_qbit_ws_full;

SET allow_experimental_nullable_tuple_type = 1;
SELECT ifNull(CAST(NULL AS Nullable(Tuple(Map(String, Nullable(QBit(Float32, 8)))))),
              CAST(tuple(map()) AS Tuple(Map(String, Nullable(QBit(Float32, 8))))));
