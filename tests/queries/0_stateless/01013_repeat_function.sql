SELECT repeat('abc', 10);
DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults
(
    strings String,
    u8 UInt8,
    u16 UInt16,
    u32 UInt32,
    u64 UInt64
)ENGINE = Memory();

INSERT INTO defaults values ('abc', 3, 12, 4, 56) ('sdfgg', 2, 10, 21, 200) ('xywq', 1, 4, 9, 5) ('plkf', 0, 5, 7,77);

SELECT repeat(strings, u8) FROM defaults;
SELECT repeat(strings, u16) FROM defaults;
SELECT repeat(strings, u32) from defaults;
SELECT repeat(strings, u64) FROM defaults;
SELECT repeat(strings, 10) FROM defaults;
SELECT repeat('abc', u8) FROM defaults;
SELECT repeat('abc', u16) FROM defaults;
SELECT repeat('abc', u32) FROM defaults;
SELECT repeat('abc', u64) FROM defaults;

SELECT repeat('Hello, world! ', 3);

DROP TABLE defaults;
