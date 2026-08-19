-- Tests for strided QBit: creation, insertion, round-trip select, type name and validation errors.

SELECT 'Round-trip QBit(Float32, 16, 8)';
DROP TABLE IF EXISTS qbit_stride;
CREATE TABLE qbit_stride (id UInt32, vec QBit(Float32, 16, 8)) ENGINE = Memory;
INSERT INTO qbit_stride VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]), (2, [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
SELECT vec FROM qbit_stride ORDER BY id;
SELECT DISTINCT toTypeName(vec) FROM qbit_stride;
DROP TABLE qbit_stride;

SELECT 'Round-trip QBit(BFloat16, 16, 8)';
CREATE TABLE qbit_stride (id UInt32, vec QBit(BFloat16, 16, 8)) ENGINE = Memory;
INSERT INTO qbit_stride VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
SELECT vec FROM qbit_stride ORDER BY id;
SELECT DISTINCT toTypeName(vec) FROM qbit_stride;
DROP TABLE qbit_stride;

SELECT 'Non-strided default (stride == dimension) hides the third argument';
SELECT toTypeName(CAST([1, 2, 3, 4]::Array(Float32) AS QBit(Float32, 4)));
SELECT toTypeName(CAST([1, 2, 3, 4]::Array(Float32) AS QBit(Float32, 4, 4)));

SELECT 'CAST Array to strided QBit (displays as the original vector)';
SELECT CAST([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]::Array(Float32) AS QBit(Float32, 16, 8));

SELECT 'Validation errors';
CREATE TABLE bad (vec QBit(Float32, 8, 0)) ENGINE = Memory; -- { serverError UNEXPECTED_AST_STRUCTURE }
CREATE TABLE bad (vec QBit(Float32, 8, 16)) ENGINE = Memory; -- { serverError UNEXPECTED_AST_STRUCTURE }
CREATE TABLE bad (vec QBit(Float32, 8, 3)) ENGINE = Memory; -- { serverError UNEXPECTED_AST_STRUCTURE }
CREATE TABLE bad (vec QBit(Float32, 12, 4)) ENGINE = Memory; -- { serverError UNEXPECTED_AST_STRUCTURE }
CREATE TABLE bad (vec QBit(Float32, 32, 12)) ENGINE = Memory; -- { serverError UNEXPECTED_AST_STRUCTURE }
