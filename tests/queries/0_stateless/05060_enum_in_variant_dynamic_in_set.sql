-- An Enum constant wrapped in Variant or Dynamic and compared against a String or FixedString column
-- used to convert to the enum's underlying number instead of its name, so IN matched the wrong row,
-- NOT IN was inverted and key analysis pruned on the wrong value. Every assertion below prints 1.
--
-- The enum is Enum8('7' = 3): the name '7' and the number 3 are both valid strings and both are stored,
-- so a cell distinguishes "returned nothing" from "returned the WRONG row". The reference for the IN
-- family is the equivalent String literal, because set construction is storage independent, so an
-- unindexed table returns the same wrong answer and could not detect the bug.

-- A remote-only plan carries no index section, so keep the read local for the pruning cells.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS carrier_str;
DROP TABLE IF EXISTS carrier_fixed;

CREATE TABLE carrier_str (v String) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO carrier_str VALUES ('7'), ('3'), ('V0'), ('zz');

CREATE TABLE carrier_fixed (v FixedString(4)) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO carrier_fixed VALUES ('7'), ('3'), ('V0'), ('zz');

SELECT 'dyn_in', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v IN ('7'));

SELECT 'dyn_not_in', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v NOT IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v NOT IN ('7'));

SELECT 'var_in', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Variant(Array(UInt8), Enum8('7' = 3)))))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v IN ('7'));

-- A different storage branch of Dynamic: the value lives in the shared variant, from which the
-- alternative is decoded rather than read from the variant list.
SELECT 'dyn_shared_variant_in', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic(max_types = 0))))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v IN ('7'));

-- A FixedString target pads the name to its width, which is a second conversion step after the name.
SELECT 'dyn_in_fixed_string', (SELECT arraySort(groupArray(v)) FROM carrier_fixed
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)))
    = (SELECT arraySort(groupArray(v)) FROM carrier_fixed WHERE v IN ('7'));

-- The set is built from a collection element rather than from the value itself.
SELECT 'dyn_collection_in', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v IN ([CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)]))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str WHERE v IN ('7'));

-- = was always correct, so it disagreeing with IN on the same constant was the signal that this is a bug.
SELECT 'dyn_eq_agrees_with_in', (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v = CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic))
    = (SELECT arraySort(groupArray(v)) FROM carrier_str
        WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)));

SELECT 'var_values', (SELECT x FROM values('x String',
    CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Variant(Array(UInt8), Enum8('7' = 3))))) = '7';

-- Correctness could also be restored by declining the index, which would silently cost pruning, so the
-- wrapped constant must prune exactly as much as the equivalent String literal.
SELECT 'dyn_still_prunes', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM carrier_str
          WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic))))
    = (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM carrier_str WHERE v IN ('7')));

-- The equality above would also hold if BOTH sides read every granule. This pins that granules really are
-- skipped, so the wrapped constant is still turned into a usable range.
SELECT 'dyn_prunes_something', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    < sum(toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM carrier_str
          WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic))));

DROP TABLE carrier_str;
DROP TABLE carrier_fixed;
