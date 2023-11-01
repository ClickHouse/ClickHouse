DROP TABLE IF EXISTS table_gcd_codec;
CREATE TABLE table_gcd_codec (str String CODEC(GCD, LZ4)) ENGINE = Memory; -- { serverError 36 }
