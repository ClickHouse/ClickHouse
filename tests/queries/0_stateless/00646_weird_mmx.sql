DROP TABLE IF EXISTS weird_mmx;

CREATE TABLE weird_mmx (x Array(UInt64)) ENGINE = TinyLog;
-- this triggers overlapping matches in LZ4 decompression routine; 915 is the minimum number
-- see comment in LZ4_decompression_faster.cpp about usage of MMX registers
INSERT INTO weird_mmx SELECT range(number % 10) FROM system.numbers LIMIT 915;
SELECT sum(length(*)) FROM weird_mmx;

DROP TABLE weird_mmx;
