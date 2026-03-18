SET allow_experimental_qbit_type = 1;

DROP TABLE IF EXISTS qbit_nullable_test;

SELECT 'Test QBit within Nullable columns';
CREATE TABLE qbit_nullable_test (id UInt32, data Nullable(QBit(Float64, 4))) ENGINE = Memory;
INSERT INTO qbit_nullable_test VALUES (1, NULL),
    (2, [5.0, 6.0, 7.0, 8.0]::QBit(Float64, 4)),
    (3, [1.5, 2.5, 3.5, 4.5]::QBit(Float64, 4)),
    (4, NULL);

SELECT id, data FROM qbit_nullable_test ORDER BY id;

DROP TABLE qbit_nullable_test;
