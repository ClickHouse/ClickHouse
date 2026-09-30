-- Tests correctness of joins on packed composite fixed keys of various widths.
-- Composite keys that fit in 4 / 8 bytes are routed to the keys32 / keys64 hash
-- maps; previously every all-fixed key up to 16 bytes was packed into keys128.
--
-- Every multi-component key is generated so that NO single component is unique,
-- but the whole tuple is a bijection over [0, 1000). Both tables hold the same
-- 1000 rows with v = number, so every inner join is strictly 1:1 over the full
-- overlap and must yield count() = 1000 and sum(l.v + r.v) = 2 * (0 + ... + 999)
-- = 999000, independent of the internal hash-map type. Because each component is
-- non-unique on its own, a bug that ignored, truncated, or dropped any single
-- component would make the key many-to-many and blow up the count.

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

-- number = 40 * intDiv(number, 40) + number % 40, etc., so each (component_a,
-- component_b[, component_c]) tuple below bijects [0, 1000) while every single
-- component repeats. The 3-component key uses the decimal digits of number.
INSERT INTO pj_left
SELECT
    toUInt16(number % 40), toUInt16(intDiv(number, 40)),
    toUInt32(number % 40), toUInt32(intDiv(number, 40)),
    toUInt32(number % 8),  toUInt8(intDiv(number, 8)),
    toFixedString(leftPad(toString(number), 4, '0'), 4),
    toUInt32(intDiv(number, 100)), toUInt32(intDiv(number, 10) % 10), toUInt32(number % 10),
    number
FROM numbers(1000);

INSERT INTO pj_right SELECT * FROM pj_left;

SELECT 'keys32_u16x2',  count(), sum(l.v + r.v) FROM pj_left l INNER JOIN pj_right r ON l.u16a = r.u16a AND l.u16b = r.u16b;
SELECT 'keys64_u32x2',  count(), sum(l.v + r.v) FROM pj_left l INNER JOIN pj_right r ON l.u32a = r.u32a AND l.u32b = r.u32b;
SELECT 'keys64_u32_u8', count(), sum(l.v + r.v) FROM pj_left l INNER JOIN pj_right r ON l.u32c = r.u32c AND l.u8c = r.u8c;
SELECT 'keys32_fixstr', count(), sum(l.v + r.v) FROM pj_left l INNER JOIN pj_right r ON l.fs = r.fs;
SELECT 'keys128_u32x3', count(), sum(l.v + r.v) FROM pj_left l INNER JOIN pj_right r ON l.u32x = r.u32x AND l.u32y = r.u32y AND l.u32z = r.u32z;

-- parallel_hash routes these through the two-level maps (two_level_keys32 / two_level_keys64). Their
-- results are checked too: an incorrect scatter selector could drop rows, which a perf query reading
-- into FORMAT Null would not catch. max_threads > 1 forces the concurrent build so the scatter across
-- sub-joins is actually exercised.
SELECT 'two_level_keys32', count(), sum(l.v + r.v) FROM pj_left l INNER JOIN pj_right r ON l.u16a = r.u16a AND l.u16b = r.u16b SETTINGS join_algorithm = 'parallel_hash', max_threads = 8;
SELECT 'two_level_keys64', count(), sum(l.v + r.v) FROM pj_left l INNER JOIN pj_right r ON l.u32a = r.u32a AND l.u32b = r.u32b SETTINGS join_algorithm = 'parallel_hash', max_threads = 8;

DROP TABLE pj_left;
DROP TABLE pj_right;
