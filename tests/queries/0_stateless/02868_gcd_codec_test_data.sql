DROP TABLE IF EXISTS table_none;
CREATE TABLE table_none (id UInt64, ui UInt256 CODEC(LZ4)) ENGINE = Memory;
INSERT INTO table_none SELECT * FROM generateRandom() LIMIT 50;

DROP TABLE IF EXISTS table_gcd_codec;
CREATE TABLE table_gcd_codec (id UInt64, ui UInt256 CODEC(GCD, LZ4)) ENGINE = Memory;
INSERT INTO table_gcd_codec SELECT * FROM table_none;

SELECT COUNT(*)
FROM (
    SELECT table_none.id, table_none.ui AS ui1, table_gcd_codec.id, table_gcd_codec.ui AS ui2
    FROM table_none
    JOIN table_gcd_codec ON table_none.id = table_gcd_codec.id
)
WHERE ui1 != ui2;
