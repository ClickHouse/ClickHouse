-- Tests that `length` of a QBit returns the dimension of the vector as a constant,
-- similarly to how `length` of a FixedString returns its size as a constant.

SELECT 'Basic';
SELECT length([1, 2, 3, 4, 5, 6, 7, 8]::QBit(Float32, 8));
SELECT length([1, 2, 3]::QBit(Float64, 3));
SELECT length([1, 2, 3, 4, 5]::QBit(BFloat16, 5));

-- Dimension that is not a multiple of 8 (no padding artifacts in the result).
SELECT length([1, 2, 3, 4, 5, 6, 7, 8, 9]::QBit(Float32, 9));

SELECT 'Is constant';
SELECT isConstant(length([1, 2, 3, 4, 5, 6, 7, 8]::QBit(Float32, 8)));

SELECT 'Aliases';
SELECT OCTET_LENGTH([1, 2, 3, 4]::QBit(Float32, 4)), CARDINALITY([1, 2, 3, 4]::QBit(Float32, 4));

SELECT 'From a table with multiple rows';
DROP TABLE IF EXISTS qbit_length_test;
CREATE TABLE qbit_length_test (id UInt32, vec QBit(Float32, 16)) ENGINE = Memory;
INSERT INTO qbit_length_test VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
INSERT INTO qbit_length_test VALUES (2, [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
INSERT INTO qbit_length_test (id) VALUES (3);
SELECT id, length(vec) FROM qbit_length_test ORDER BY id;
DROP TABLE qbit_length_test;

SELECT 'Nullable QBit';
SELECT length(CAST([1, 2, 3]::QBit(Float32, 3) AS Nullable(QBit(Float32, 3))));
SELECT length(CAST(NULL AS Nullable(QBit(Float32, 3))));
