SET allow_experimental_qbit_type = 1;
SET allow_experimental_dynamic_type = 1;

DROP TABLE IF EXISTS qbit_dynamic_test;


SELECT 'Test QBit within Dynamic columns';
CREATE TABLE qbit_dynamic_test (id UInt32, data Dynamic) ENGINE = Memory;
INSERT INTO qbit_dynamic_test VALUES
    (1, [1.0, 2.0, 3.0, 4.0]::QBit(Float32, 4)),
    (2, [5.0, 6.0, 7.0, 8.0]::QBit(Float64, 4)),
    (3, [1.5, 2.5, 3.5, 4.5]::QBit(BFloat16, 4));
SELECT id, data, dynamicType(data) as type FROM qbit_dynamic_test ORDER BY id;


SELECT 'Mixed Dynamic column with QBit and other types';
INSERT INTO qbit_dynamic_test VALUES
    (4, 'string_value'),
    (5, 42),
    (6, [9.0, 10.0, 11.0, 12.0]::QBit(Float32, 4));
SELECT id, data, dynamicType(data) as type FROM qbit_dynamic_test ORDER BY id;


DROP TABLE qbit_dynamic_test;
