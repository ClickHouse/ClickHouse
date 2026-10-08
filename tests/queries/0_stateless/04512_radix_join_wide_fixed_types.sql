-- Tags: no-random-settings
-- Wide fixed-width types through the radix join: 32-byte keys (`UInt256`, `Int256`, `Decimal256`)
-- take the width-32 scatter/histogram kernels, and FixedString payloads of generic widths (7 and
-- 33 bytes, not in the {1,2,4,8,16,32} dispatch set) take the runtime-width scatter kernel. Every
-- comparison must equal `hash` on (count, value fingerprint); the engagement assertions below
-- prove the radix path actually ran (a silent planner fallback would make the comparisons
-- vacuously green). `max_threads = 4` pins the plan: fanout = bit_ceil(max_threads) = 4, the tiny
-- build stays within the leaf byte budget (single pass), and 100 distinct build keys spread over
-- 4 partitions leave no leaf empty, so `RadixHashJoinLeafGroupBuilds` is exactly 4 per radix run.
-- The old analyzer was intentionally not taught `radix_join`, hence enable_analyzer = 1.
SET enable_analyzer = 1;
SET max_threads = 4;

DROP TABLE IF EXISTS rhw_b;
DROP TABLE IF EXISTS rhw_p;

CREATE TABLE rhw_b (u256 UInt256, i256 Int256, d256 Decimal256(40), f7 FixedString(7), f33 FixedString(33), a UInt64, pay UInt64) ENGINE = Memory;
CREATE TABLE rhw_p (u256 UInt256, i256 Int256, d256 Decimal256(40), f7 FixedString(7), f33 FixedString(33), a UInt64, pay UInt64) ENGINE = Memory;

-- Overlapping but unequal key ranges with duplicates (many-to-many), every key column a function of
-- one base key so all shapes share the same match structure. The 256-bit keys carry entropy in the
-- high 128 bits too (a zero-padded number would leave 24 of the 32 key bytes constant).
INSERT INTO rhw_b SELECT
    bitShiftLeft(toUInt256(cityHash64(number % 100, 1)), 128) + toUInt256(number % 100),
    toInt256(bitShiftLeft(toUInt256(cityHash64(number % 100, 2)), 120)) + toInt256(number % 100),
    toDecimal256(number % 100, 40),
    toFixedString(leftPad(toString(number % 100), 7, 'a'), 7),
    toFixedString(leftPad(toString(number % 100), 33, 'b'), 33),
    number % 100,
    number
FROM numbers(300);
INSERT INTO rhw_p SELECT
    bitShiftLeft(toUInt256(cityHash64(number % 150, 1)), 128) + toUInt256(number % 150),
    toInt256(bitShiftLeft(toUInt256(cityHash64(number % 150, 2)), 120)) + toInt256(number % 150),
    toDecimal256(number % 150, 40),
    toFixedString(leftPad(toString(number % 150), 7, 'a'), 7),
    toFixedString(leftPad(toString(number % 150), 33, 'b'), 33),
    number % 150,
    number
FROM numbers(200);

-- 32-byte single keys (gate ACCEPT: fixed width 32, multiple of 4, <= 64).
SELECT 'single_u256', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhw_p AS p INNER JOIN rhw_b AS b ON p.u256 = b.u256 SETTINGS join_algorithm = 'radix_join')
                    = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhw_p AS p INNER JOIN rhw_b AS b ON p.u256 = b.u256 SETTINGS join_algorithm = 'hash')
SETTINGS log_comment = '04512_radix_wide_single_u256';

SELECT 'single_i256', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhw_p AS p INNER JOIN rhw_b AS b ON p.i256 = b.i256 SETTINGS join_algorithm = 'radix_join')
                    = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhw_p AS p INNER JOIN rhw_b AS b ON p.i256 = b.i256 SETTINGS join_algorithm = 'hash');

SELECT 'single_d256', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhw_p AS p INNER JOIN rhw_b AS b ON p.d256 = b.d256 SETTINGS join_algorithm = 'radix_join')
                    = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhw_p AS p INNER JOIN rhw_b AS b ON p.d256 = b.d256 SETTINGS join_algorithm = 'hash');

-- Composite (u256, u64): packed width 40, still accepted; the 32-byte key column takes the
-- generic composite fold and is scattered as a width-32 payload.
SELECT 'composite_u256_u64', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhw_p AS p INNER JOIN rhw_b AS b ON p.u256 = b.u256 AND p.a = b.a SETTINGS join_algorithm = 'radix_join')
                           = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhw_p AS p INNER JOIN rhw_b AS b ON p.u256 = b.u256 AND p.a = b.a SETTINGS join_algorithm = 'hash');

-- Wide payloads through the radix scatter: widths 7 and 33 (generic kernel) and 32 (width-32
-- kernel) from both sides, fingerprinted byte-exactly.
SELECT 'wide_payloads', (SELECT (count(), sum(cityHash64(p.pay, b.pay, p.f7, b.f7, p.f33, b.f33, p.u256, b.u256))) FROM rhw_p AS p INNER JOIN rhw_b AS b ON p.a = b.a SETTINGS join_algorithm = 'radix_join')
                      = (SELECT (count(), sum(cityHash64(p.pay, b.pay, p.f7, b.f7, p.f33, b.f33, p.u256, b.u256))) FROM rhw_p AS p INNER JOIN rhw_b AS b ON p.a = b.a SETTINGS join_algorithm = 'hash')
SETTINGS log_comment = '04512_radix_wide_payloads';

-- Engagement assertions (see the header comment for the plan arithmetic behind the value 4).
SYSTEM FLUSH LOGS query_log;

SELECT
    'single_u256_engaged',
    ProfileEvents['RadixHashJoinLeafGroupBuilds']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04512_radix_wide_single_u256'
    AND event_date >= yesterday()
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT
    'wide_payloads_engaged',
    ProfileEvents['RadixHashJoinLeafGroupBuilds']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04512_radix_wide_payloads'
    AND event_date >= yesterday()
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE rhw_b;
DROP TABLE rhw_p;
