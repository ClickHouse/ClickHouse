DROP TABLE IF EXISTS test.weird_mmx;

CREATE TABLE test.weird_mmx (x Array(UInt64)) ENGINE = TinyLog;
-- this triggers overlapping matches in LZ4 decompression routine; 915 is the minimum number
-- see comment in LZ4_decompression_faster.cpp about usage of MMX registers
INSERT INTO test.weird_mmx SELECT range(number % 10) FROM system.numbers LIMIT 915;
SELECT sum(length(*)) FROM test.weird_mmx;

DROP TABLE test.weird_mmx;
