-- Test: exercises `checkTupleNames` DUPLICATE_COLUMN branch with duplicate Tuple element names.
-- Covers: src/DataTypes/DataTypeTuple.cpp:69 — `if (!names_set.insert(name).second)` rejects
-- a Tuple type with two elements that share the same name. Without this check, an explicit
-- Tuple(a Int, a Int) would be silently accepted, producing the same class of subcolumn
-- ambiguity that PR #98377 fixes for the reserved name 'null'.

SELECT CAST((1, 2), 'Tuple(a Int32, a Int32)'); -- { serverError DUPLICATE_COLUMN }
SELECT CAST((1, 2), 'Tuple(x UInt8, x UInt8)'); -- { serverError DUPLICATE_COLUMN }
SELECT CAST((1, 'a', 2), 'Tuple(c Int, b String, c Int)'); -- { serverError DUPLICATE_COLUMN }

-- Also via CREATE TABLE — the type validator runs at table-creation time.
DROP TABLE IF EXISTS test_tuple_duplicate;
CREATE TABLE test_tuple_duplicate (t Tuple(a UInt32, a UInt32)) ENGINE = Memory; -- { serverError DUPLICATE_COLUMN }

-- Sanity: distinct element names in the same Tuple are accepted.
SELECT CAST((1, 2), 'Tuple(a Int32, b Int32)');
