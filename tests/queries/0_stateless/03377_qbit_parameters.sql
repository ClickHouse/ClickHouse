-- Tests for QBit query parameters
-- https://github.com/ClickHouse/ClickHouse/issues/91103

SET allow_experimental_qbit_type = 1;

SELECT 'Test QBit query parameters';
SET param_q1 = [1, 2, 3, 4];
SELECT {q1:QBit(Float32, 4)};

SET param_q2 = [1.5, 2.5, 3.5, 4.5];
SELECT {q2:QBit(Float64, 4)};

SET param_q3 = [1, 2, 3, 4, 5, 6, 7, 8];
SELECT {q3:QBit(BFloat16, 8)};

SELECT 'Test QBit query parameters with different dimensions';
SET param_q4 = [1, 2];
SELECT {q4:QBit(Float32, 2)};

SET param_q5 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
SELECT {q5:QBit(Float32, 16)};

SELECT 'Test QBit query parameters in expressions';
SET param_q6 = [1, 2, 3, 4];
SELECT L2DistanceTransposed({q6:QBit(Float32, 4)}, [1, 2, 3, 4], 32);

SELECT 'Test QBit query parameters with table';
DROP TABLE IF EXISTS qbit_param_test;
CREATE TABLE qbit_param_test (id UInt32, vec QBit(Float32, 4)) ENGINE = Memory;
INSERT INTO qbit_param_test VALUES (1, [1, 0, 0, 0]), (2, [0, 1, 0, 0]), (3, [1, 1, 1, 1]);

SET param_q7 = [1, 1, 1, 1];
SELECT id, L2DistanceTransposed(vec, {q7:Array(Float32)}, 4) AS dist FROM qbit_param_test ORDER BY id;

DROP TABLE qbit_param_test;
