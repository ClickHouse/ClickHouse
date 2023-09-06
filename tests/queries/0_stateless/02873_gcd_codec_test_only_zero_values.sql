CREATE TABLE table_gcd_codec_only_zero_values (n UInt8 CODEC(GCD, LZ4)) ENGINE = Memory;
INSERT INTO table_gcd_codec_only_zero_values VALUES (0), (0), (0);
SELECT * FROM table_gcd_codec_only_zero_values;
