SELECT space(3);
DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults
(
    u8 UInt8,
    u16 UInt16,
    u32 UInt32,
    u64 UInt64
)ENGINE = Memory();

INSERT INTO defaults values (3, 12, 4, 56) (2, 10, 21, 20) (1, 4, 9, 5) (0, 5, 7,7);

SELECT space(u8), length(space(u8)) FROM defaults;
SELECT space(u16), length(space(u16)) FROM defaults;
SELECT space(u32), length(space(u32)) from defaults;
SELECT space(u64), length(space(u64)) FROM defaults;
SELECT space(NULL) FROM default;

DROP TABLE defaults;
