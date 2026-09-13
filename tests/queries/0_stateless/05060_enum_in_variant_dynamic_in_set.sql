-- An `Enum` constant wrapped in `Variant` or `Dynamic` and compared against a `String` or
-- `FixedString` column used to convert to the enum's underlying number instead of its name, so `IN`
-- matched the wrong row, `NOT IN` was inverted and key analysis pruned on the wrong value. Every
-- assertion below prints 1.
--
-- The enum is `Enum8('7' = 3)`: the name `'7'` and the number `3` are both valid strings and both are
-- stored, so a cell distinguishes "returned nothing" from "returned the WRONG row". The reference for
-- the `IN` family is the equivalent `String` literal, because set construction is storage independent,
-- so an unindexed table returns the same wrong answer and could not detect the bug (`ENGINE = Log` is
-- equally wrong).

-- A remote-only plan carries no index section, so keep the read local for the pruning cells.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS carrier_str;
DROP TABLE IF EXISTS carrier_fixed;
DROP TABLE IF EXISTS carrier_lc;
DROP TABLE IF EXISTS carrier_bool;
DROP TABLE IF EXISTS carrier_date;

CREATE TABLE carrier_str (v String) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO carrier_str VALUES ('7'), ('3'), ('V0'), ('zz');

CREATE TABLE carrier_fixed (v FixedString(4)) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO carrier_fixed VALUES ('7'), ('3'), ('V0'), ('zz');

CREATE TABLE carrier_lc (v LowCardinality(String)) ENGINE = MergeTree ORDER BY v;
INSERT INTO carrier_lc VALUES ('7'), ('3');

CREATE TABLE carrier_bool (v String) ENGINE = MergeTree ORDER BY v;
INSERT INTO carrier_bool VALUES ('true'), ('1'), ('false'), ('0');

CREATE TABLE carrier_date (d Date) ENGINE = MergeTree ORDER BY d;
INSERT INTO carrier_date VALUES ('2020-01-02'), ('2020-01-03');

SELECT 'dyn_in', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v IN ('7'));

SELECT 'dyn_not_in', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v NOT IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v NOT IN ('7'));

SELECT 'var_in', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Variant(Array(UInt8), Enum8('7' = 3)))))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v IN ('7'));

-- A different storage branch of `Dynamic`: the value lives in the shared variant, from which the
-- alternative is decoded rather than read from the variant list.
SELECT 'dyn_shared_variant_in', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic(max_types = 0))))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v IN ('7'));

-- A `FixedString` target pads the name to its width, which is a second conversion step after the name.
SELECT 'dyn_in_fixed_string', (SELECT arraySort(groupArray(v)) FROM carrier_fixed
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)))
    = (SELECT arraySort(groupArray(v)) FROM carrier_fixed WHERE v IN ('7'));

-- The set is built from a collection member rather than from the value itself.
SELECT 'dyn_list_in', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic), 'zz'))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v IN ('7', 'zz'));

-- `=` was always correct, so it disagreeing with `IN` on the same constant was the signal that this is
-- a bug.
SELECT 'dyn_eq_agrees_with_in', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v = CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)));

SELECT 'var_values', (SELECT x FROM values('x String',
    CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Variant(Array(UInt8), Enum8('7' = 3))))) = '7';

-- `Enum16` is the same defect at a different width, and the fix is width-agnostic.
SELECT 'dyn_in_enum16', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v IN (CAST(CAST('7', 'Enum16(\'7\' = 3)') AS Dynamic)))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v IN ('7'));

-- A `LowCardinality(String)` target is reached through the same conversion.
SELECT 'dyn_in_lowcardinality', (SELECT arraySort(groupArray(v)) FROM carrier_lc
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)))
    = (SELECT arraySort(groupArray(v)) FROM carrier_lc WHERE v IN ('7'));

-- An `Enum` is not the only tag a carrier records. `Variant(Bool, UInt8)` is ambiguous, so the
-- reconstructed value alone cannot say which alternative was active, while the discriminator can.
SELECT 'var_in_bool', (SELECT arraySort(groupArray(v)) FROM carrier_bool
        WHERE v IN (CAST(CAST(true, 'Bool') AS Variant(Bool, UInt8))))
    = (SELECT arraySort(groupArray(v)) FROM carrier_bool WHERE v IN (CAST(true, 'Bool')))
    SETTINGS allow_suspicious_variant_types = 1;

-- Negative control for the cell above: the same ambiguous `Variant` holding a `UInt8` must keep
-- rendering as a number, so the conversion follows the active alternative and is not a blanket `Bool`
-- coercion. This cell holds before the fix as well.
SELECT 'var_in_uint8_stays_numeric', (SELECT arraySort(groupArray(v)) FROM carrier_bool
        WHERE v IN (CAST(CAST(1, 'UInt8') AS Variant(Bool, UInt8))))
    = (SELECT arraySort(groupArray(v)) FROM carrier_bool WHERE v IN (CAST(1, 'UInt8')))
    SETTINGS allow_suspicious_variant_types = 1;

-- A `Date` bound given as a `DateTime` wrapped in `Dynamic` used to match nothing at all, because the
-- day-truncating conversion is also keyed on the source type.
SELECT 'dyn_date_from_datetime', (SELECT arraySort(groupArray(d)) FROM carrier_date
        WHERE d IN (CAST(CAST('2020-01-02 05:00:00', 'DateTime(\'UTC\')') AS Dynamic)))
    = (SELECT arraySort(groupArray(d)) FROM carrier_date
        WHERE d IN (CAST('2020-01-02 05:00:00', 'DateTime(\'UTC\')')));

-- The two analyzers build a constant set through different code, so pin both here rather than relying
-- on the one CI job that still runs the old one.
SET enable_analyzer = 0;

SELECT 'dyn_in_old', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v IN ('7'));

SELECT 'dyn_not_in_old', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v NOT IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v NOT IN ('7'));

SELECT 'var_in_old', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Variant(Array(UInt8), Enum8('7' = 3)))))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v IN ('7'));

SELECT 'dyn_list_in_old', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic), 'zz'))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v IN ('7', 'zz'));

SELECT 'var_values_old', (SELECT x FROM values('x String',
    CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Variant(Array(UInt8), Enum8('7' = 3))))) = '7';

SET enable_analyzer = 1;

-- Correctness could also be restored by declining the index, which would silently cost pruning, so the
-- wrapped constant must prune exactly as much as the equivalent `String` literal.
SELECT 'dyn_still_prunes', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM carrier_str
          WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic))))
    = (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM carrier_str WHERE v IN ('7')));

-- The equality above would also hold if BOTH sides read every granule. This pins that granules really
-- are skipped, so the wrapped constant is still turned into a usable range. This cell holds before the
-- fix as well.
SELECT 'dyn_prunes_something', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    < sum(toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM carrier_str
          WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic))));

DROP TABLE carrier_str;
DROP TABLE carrier_fixed;
DROP TABLE carrier_lc;
DROP TABLE carrier_bool;
DROP TABLE carrier_date;
