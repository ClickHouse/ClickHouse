DROP TABLE IF EXISTS table_gcd_codec;
CREATE TABLE table_gcd_codec (str UInt64 CODEC(GCD)) ENGINE = Memory; -- { serverError 36 }
