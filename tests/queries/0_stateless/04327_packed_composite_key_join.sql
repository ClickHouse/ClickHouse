-- Tests correctness of joins on packed composite fixed keys of various widths.
-- Composite keys that fit in 4 / 8 bytes are routed to the keys32 / keys64 hash
-- maps; previously every all-fixed key up to 16 bytes was packed into keys128.
-- Each table holds 1000 rows with unique composite keys and v = w = number, and
-- both sides are identical, so every inner join is strictly 1:1 over the full
-- overlap. The result must therefore always be: count() = 1000 and
-- sum(v + w) = 2 * (0 + 1 + ... + 999) = 999000, independent of the internal
-- hash-map type. A bug that dropped part of the key (collisions) would change
-- the count; a bug that lost high-order bytes would change it too.

DROP TABLE IF EXISTS pj_left;
DROP TABLE IF EXISTS pj_right;

CREATE TABLE pj_left
(
    u16a UInt16, u16b UInt16,           -- (UInt16, UInt16) -> 4 bytes -> keys32
    u32a UInt32, u32b UInt32,           -- (UInt32, UInt32) -> 8 bytes -> keys64
    u32c UInt32, u8c UInt8,             -- (UInt32, UInt8)  -> 5 bytes -> keys64
    fs FixedString(4),                  -- single FixedString(4)        -> keys32
    u32x UInt32, u32y UInt32, u32z UInt32, -- (UInt32 x 3) -> 12 bytes -> keys128 (control)
    v UInt64
) ENGINE = Memory;

CREATE TABLE pj_right AS pj_left;

-- intDiv(number, 32) and number % 32 form a bijection for number in [0, 1000),
-- so the composite keys are unique and include swapped pairs like (1, 2)/(2, 1).
INSERT INTO pj_left
SELECT
    toUInt16(intDiv(number, 32)), toUInt16(number % 32),
    toUInt32(number), toUInt32(number * 7 + 1),
    toUInt32(number), toUInt8(number % 256),
    toFixedString(leftPad(toString(number), 4, '0'), 4),
    toUInt32(number), toUInt32(number * 3 + 1), toUInt32(number * 5 + 2),
    number
FROM numbers(1000);

INSERT INTO pj_right SELECT * FROM pj_left;

SELECT 'keys32_u16x2',  count(), sum(l.v + r.v) FROM pj_left l INNER JOIN pj_right r ON l.u16a = r.u16a AND l.u16b = r.u16b;
SELECT 'keys64_u32x2',  count(), sum(l.v + r.v) FROM pj_left l INNER JOIN pj_right r ON l.u32a = r.u32a AND l.u32b = r.u32b;
SELECT 'keys64_u32_u8', count(), sum(l.v + r.v) FROM pj_left l INNER JOIN pj_right r ON l.u32c = r.u32c AND l.u8c = r.u8c;
SELECT 'keys32_fixstr', count(), sum(l.v + r.v) FROM pj_left l INNER JOIN pj_right r ON l.fs = r.fs;
SELECT 'keys128_u32x3', count(), sum(l.v + r.v) FROM pj_left l INNER JOIN pj_right r ON l.u32x = r.u32x AND l.u32y = r.u32y AND l.u32z = r.u32z;

DROP TABLE pj_left;
DROP TABLE pj_right;
