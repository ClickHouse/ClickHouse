-- Padded comparison semantics must survive both ways of transforming constants into key values.
-- `toString` isolates monotonic transforms for inequalities; `reverse` isolates deterministic
-- transforms for equality. The byte strings differ only in padding, so losing a match is visible.
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;

DROP TABLE IF EXISTS erased_fixed_monotonic;
DROP TABLE IF EXISTS erased_fixed_deterministic;
DROP TABLE IF EXISTS erased_fixed_wide;
DROP TABLE IF EXISTS erased_fixed_numeric;

CREATE TABLE erased_fixed_monotonic (s String) ENGINE = MergeTree ORDER BY toString(s)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE erased_fixed_deterministic (s String) ENGINE = MergeTree ORDER BY reverse(s)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO erased_fixed_monotonic VALUES ('V0'), ('V0\0'), ('V0\0\0'), ('V0X'), ('X');
INSERT INTO erased_fixed_deterministic SELECT * FROM erased_fixed_monotonic;

SET analyze_index_with_multiple_key_columns_per_condition = 0;

SELECT '0: full FixedString';
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_monotonic WHERE s <= toFixedString('V0', 2);
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_deterministic WHERE s = toFixedString('V0', 2);
SELECT count() FROM erased_fixed_deterministic WHERE s != toFixedString('V0', 2);
SELECT count() FROM erased_fixed_monotonic WHERE s <= toFixedString('V0', 2)
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM erased_fixed_deterministic WHERE s = toFixedString('V0', 2)
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

SELECT '0: Variant';
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_monotonic WHERE s <= CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64));
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_deterministic WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64));
SELECT count() FROM erased_fixed_deterministic WHERE s != CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64));
SELECT count() FROM erased_fixed_monotonic WHERE s <= CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64))
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM erased_fixed_deterministic WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64))
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

SELECT '0: full Dynamic';
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_monotonic WHERE s <= CAST(toFixedString('V0', 2) AS Dynamic);
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_deterministic WHERE s = CAST(toFixedString('V0', 2) AS Dynamic);
SELECT count() FROM erased_fixed_deterministic WHERE s != CAST(toFixedString('V0', 2) AS Dynamic);
SELECT count() FROM erased_fixed_monotonic WHERE s <= CAST(toFixedString('V0', 2) AS Dynamic)
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM erased_fixed_deterministic WHERE s = CAST(toFixedString('V0', 2) AS Dynamic)
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

SELECT '0: shared Dynamic';
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_monotonic WHERE s <= CAST(toFixedString('V0', 3) AS Dynamic(max_types = 0));
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_deterministic WHERE s = CAST(toFixedString('V0', 3) AS Dynamic(max_types = 0));
SELECT count() FROM erased_fixed_deterministic WHERE s != CAST(toFixedString('V0', 3) AS Dynamic(max_types = 0));
SELECT count() FROM erased_fixed_monotonic WHERE s <= CAST(toFixedString('V0', 3) AS Dynamic(max_types = 0))
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM erased_fixed_deterministic WHERE s = CAST(toFixedString('V0', 3) AS Dynamic(max_types = 0))
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

-- An active `String` compares bytewise, so both transforms still provide pruning.
SELECT count() FROM erased_fixed_monotonic WHERE s <= CAST('V0' AS Dynamic)
SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT count() FROM erased_fixed_deterministic WHERE s = CAST('V0' AS Variant(String, UInt64))
SETTINGS force_primary_key = 1, max_rows_to_read = 3;

SET analyze_index_with_multiple_key_columns_per_condition = 1;

SELECT '1: full FixedString';
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_monotonic WHERE s <= toFixedString('V0', 2);
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_deterministic WHERE s = toFixedString('V0', 2);
SELECT count() FROM erased_fixed_deterministic WHERE s != toFixedString('V0', 2);
SELECT count() FROM erased_fixed_monotonic WHERE s <= toFixedString('V0', 2)
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM erased_fixed_deterministic WHERE s = toFixedString('V0', 2)
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

SELECT '1: Variant';
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_monotonic WHERE s <= CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64));
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_deterministic WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64));
SELECT count() FROM erased_fixed_deterministic WHERE s != CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64));
SELECT count() FROM erased_fixed_monotonic WHERE s <= CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64))
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM erased_fixed_deterministic WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64))
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

SELECT '1: full Dynamic';
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_monotonic WHERE s <= CAST(toFixedString('V0', 2) AS Dynamic);
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_deterministic WHERE s = CAST(toFixedString('V0', 2) AS Dynamic);
SELECT count() FROM erased_fixed_deterministic WHERE s != CAST(toFixedString('V0', 2) AS Dynamic);
SELECT count() FROM erased_fixed_monotonic WHERE s <= CAST(toFixedString('V0', 2) AS Dynamic)
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM erased_fixed_deterministic WHERE s = CAST(toFixedString('V0', 2) AS Dynamic)
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

SELECT '1: shared Dynamic';
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_monotonic WHERE s <= CAST(toFixedString('V0', 3) AS Dynamic(max_types = 0));
SELECT arraySort(groupArray(hex(s))) FROM erased_fixed_deterministic WHERE s = CAST(toFixedString('V0', 3) AS Dynamic(max_types = 0));
SELECT count() FROM erased_fixed_deterministic WHERE s != CAST(toFixedString('V0', 3) AS Dynamic(max_types = 0));
SELECT count() FROM erased_fixed_monotonic WHERE s <= CAST(toFixedString('V0', 3) AS Dynamic(max_types = 0))
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM erased_fixed_deterministic WHERE s = CAST(toFixedString('V0', 3) AS Dynamic(max_types = 0))
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

-- An active `String` compares bytewise, so both transforms still provide pruning.
SELECT count() FROM erased_fixed_monotonic WHERE s <= CAST('V0' AS Dynamic)
SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT count() FROM erased_fixed_deterministic WHERE s = CAST('V0' AS Variant(String, UInt64))
SETTINGS force_primary_key = 1, max_rows_to_read = 3;

-- A wide `FixedString` input has only one key value for the constant's padded equivalence class.
CREATE TABLE erased_fixed_wide (s FixedString(8)) ENGINE = MergeTree ORDER BY reverse(s)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO erased_fixed_wide SELECT * FROM erased_fixed_monotonic;
SELECT count() FROM erased_fixed_wide
WHERE s = CAST(toFixedString('V0', 3) AS Dynamic)
SETTINGS force_primary_key = 1, max_rows_to_read = 4;

-- A numeric input parses unpadded text rather than comparing padded byte strings.
CREATE TABLE erased_fixed_numeric (n UInt64) ENGINE = MergeTree ORDER BY n + 1
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO erased_fixed_numeric VALUES (1), (12), (123), (1234), (12345);
SELECT count() FROM erased_fixed_numeric WHERE n = toFixedString('123', 3)
SETTINGS force_primary_key = 1, max_rows_to_read = 3;

DROP TABLE erased_fixed_monotonic;
DROP TABLE erased_fixed_deterministic;
DROP TABLE erased_fixed_wide;
DROP TABLE erased_fixed_numeric;
