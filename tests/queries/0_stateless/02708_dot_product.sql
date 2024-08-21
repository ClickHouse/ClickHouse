SELECT dotProduct([12, 2.22, 302], [1.32, 231.2, 11.1]);

SELECT scalarProduct([12, 2.22, 302], [1.32, 231.2, 11.1]);

SELECT arrayDotProduct([12, 2.22, 302], [1.32, 231.2, 11.1]);

SELECT dotProduct([1.3, 2, 3, 4, 5], [222, 12, 5.3, 2, 8]);

SELECT dotProduct([1, 1, 1, 1, 1], [222, 12, 0, -12, 8]);

SELECT round(dotProduct([12345678901234567], [1]) - dotProduct(tuple(12345678901234567), tuple(1)), 2);

SELECT round(dotProduct([-1, 2, 3.002], [2, 3.4, 4]) - dotProduct((-1, 2, 3.002), (2, 3.4, 4)), 2);

DROP TABLE IF EXISTS product_fp64_fp64;
CREATE TABLE product_fp64_fp64 (x Array(Float64), y Array(Float64)) engine = MergeTree() order by x;
INSERT INTO TABLE product_fp64_fp64 (x, y) values ([1, 2], [3, 4]);
SELECT toTypeName(dotProduct(x, y)) from product_fp64_fp64;
DROP TABLE product_fp64_fp64;

DROP TABLE IF EXISTS product_fp32_fp32;
CREATE TABLE product_fp32_fp32 (x Array(Float32), y Array(Float32)) engine = MergeTree() order by x;
INSERT INTO TABLE product_fp32_fp32 (x, y) values ([1, 2], [3, 4]);
SELECT toTypeName(dotProduct(x, y)) from product_fp32_fp32;
DROP TABLE product_fp32_fp32;

DROP TABLE IF EXISTS product_fp32_fp64;
CREATE TABLE product_fp32_fp64 (x Array(Float32), y Array(Float64)) engine = MergeTree() order by x;
INSERT INTO TABLE product_fp32_fp64 (x, y) values ([1, 2], [3, 4]);
SELECT toTypeName(dotProduct(x, y)) from product_fp32_fp64;
DROP TABLE product_fp32_fp64;

DROP TABLE IF EXISTS product_uint8_fp64;
CREATE TABLE product_uint8_fp64 (x Array(UInt8), y Array(Float64)) engine = MergeTree() order by x;
INSERT INTO TABLE product_uint8_fp64 (x, y) values ([1, 2], [3, 4]);
SELECT toTypeName(dotProduct(x, y)) from product_uint8_fp64;
DROP TABLE product_uint8_fp64;

DROP TABLE IF EXISTS product_uint8_uint8;
CREATE TABLE product_uint8_uint8 (x Array(UInt8), y Array(UInt8)) engine = MergeTree() order by x;
INSERT INTO TABLE product_uint8_uint8 (x, y) values ([1, 2], [3, 4]);
SELECT toTypeName(dotProduct(x, y)) from product_uint8_uint8;
DROP TABLE product_uint8_uint8;

DROP TABLE IF EXISTS product_uint64_uint64;
CREATE TABLE product_uint64_uint64 (x Array(UInt64), y Array(UInt64)) engine = MergeTree() order by x;
INSERT INTO TABLE product_uint64_uint64 (x, y) values ([1, 2], [3, 4]);
SELECT toTypeName(dotProduct(x, y)) from product_uint64_uint64;
DROP TABLE product_uint64_uint64;

DROP TABLE IF EXISTS product_int32_uint64;
CREATE TABLE product_int32_uint64 (x Array(Int32), y Array(UInt64)) engine = MergeTree() order by x;
INSERT INTO TABLE product_int32_uint64 (x, y) values ([1, 2], [3, 4]);
SELECT toTypeName(dotProduct(x, y)) from product_int32_uint64;
DROP TABLE product_int32_uint64;
