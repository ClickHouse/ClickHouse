-- Tags: no-parallel
SET allow_experimental_named_scalars = 1;

DROP NAMED SCALAR IF EXISTS cv_test;
DROP NAMED SCALAR IF EXISTS cv_ref;
DROP NAMED SCALAR IF EXISTS cv_big;
DROP NAMED SCALAR IF EXISTS cv_fixed;
DROP NAMED SCALAR IF EXISTS cv_const;
DROP NAMED SCALAR IF EXISTS cv_local;

-- Basic CRUD: CREATE, value/type, system table row.
CREATE NAMED SCALAR cv_test AS SELECT toUInt32(1);
SELECT getNamedScalar('cv_test');
SELECT name, kind, value, type, has_value, current_value_is_valid
FROM system.named_scalars WHERE name = 'cv_test' ORDER BY name;

-- OR REPLACE swaps cleanly across incompatible types.
CREATE OR REPLACE NAMED SCALAR cv_test AS SELECT toUInt32(2);
SELECT getNamedScalar('cv_test');
CREATE OR REPLACE NAMED SCALAR cv_test AS SELECT 'x';
SELECT getNamedScalar('cv_test');
SELECT toTypeName(getNamedScalar('cv_test'));

-- Self-reference and SQL SECURITY non-DEFINER are rejected.
CREATE NAMED SCALAR cv_ref AS SELECT getNamedScalar('cv_test'); -- {serverError BAD_ARGUMENTS}
CREATE NAMED SCALAR cv_ref AS SELECT getNamedScalarOrDefault('cv_test', toUInt32(0)); -- {serverError BAD_ARGUMENTS}
CREATE NAMED SCALAR cv_ref SQL SECURITY INVOKER AS SELECT toUInt32(1); -- {serverError BAD_ARGUMENTS}
CREATE NAMED SCALAR cv_ref SQL SECURITY NONE AS SELECT toUInt32(1); -- {serverError BAD_ARGUMENTS}

-- Missing scalar / duplicate name / IF NOT EXISTS.
SELECT getNamedScalar('missing'); -- {serverError NAMED_SCALAR_NOT_FOUND}
CREATE NAMED SCALAR cv_test AS SELECT toUInt32(3); -- {serverError NAMED_SCALAR_ALREADY_EXISTS}
CREATE NAMED SCALAR IF NOT EXISTS cv_test AS SELECT toUInt32(4);
SELECT getNamedScalar('cv_test');

DROP NAMED SCALAR cv_test;
DROP NAMED SCALAR cv_test; -- {serverError NAMED_SCALAR_NOT_FOUND}
DROP NAMED SCALAR IF EXISTS cv_test;

-- Value-size cap.
CREATE NAMED SCALAR cv_big AS SELECT repeat('x', 2 * 1024 * 1024); -- {serverError TOO_LARGE_STRING_SIZE}

-- Existence check fires before SELECT eval, so IF NOT EXISTS / duplicate
-- are no-ops / clean errors even when the new SELECT is invalid.
CREATE NAMED SCALAR cv_fixed AS SELECT toUInt32(1);
CREATE NAMED SCALAR IF NOT EXISTS cv_fixed AS (SELECT * FROM nonexistent_table);
SELECT getNamedScalar('cv_fixed');
CREATE NAMED SCALAR cv_fixed AS (SELECT * FROM nonexistent_table); -- {serverError NAMED_SCALAR_ALREADY_EXISTS}
SELECT getNamedScalar('cv_fixed');
DROP NAMED SCALAR cv_fixed;

-- Constant SELECTs are allowed for non-refreshable scalars.
CREATE NAMED SCALAR cv_const AS SELECT 42;
SELECT getNamedScalar('cv_const');
DROP NAMED SCALAR cv_const;

-- LOCAL is an explicit cache-kind modifier (equivalent to default).
CREATE LOCAL NAMED SCALAR cv_local AS SELECT toUInt16(7);
SELECT getNamedScalar('cv_local');
DROP NAMED SCALAR cv_local;

-- ----- getNamedScalarOrDefault paths (was 03803) -----
DROP NAMED SCALAR IF EXISTS cv_defined;
CREATE NAMED SCALAR cv_defined AS SELECT toUInt32(7);
SELECT getNamedScalarOrDefault('cv_defined', toUInt32(0));
SELECT getNamedScalarOrDefault('missing', toUInt32(42));
SELECT getNamedScalarOrDefault('missing', 'fallback');
SELECT getNamedScalarOrDefault('missing', NULL) IS NULL;
SELECT getNamedScalarOrDefault('missing', toUInt32(11));
DROP NAMED SCALAR cv_defined;

-- ----- CREATE rollback when initial SELECT throws (was 03811) -----
DROP NAMED SCALAR IF EXISTS cv_rollback;
CREATE NAMED SCALAR cv_rollback AS SELECT throwIf(1, 'eval_failed'); -- {serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO}
-- Catalog must be empty for this name.
SELECT count() FROM system.named_scalars WHERE name = 'cv_rollback';
SELECT getNamedScalar('cv_rollback'); -- {serverError NAMED_SCALAR_NOT_FOUND}
-- Retry with a working SELECT must succeed (orphan blob from first attempt is cleaned up).
CREATE NAMED SCALAR cv_rollback AS SELECT toUInt32(7);
SELECT getNamedScalar('cv_rollback');
DROP NAMED SCALAR cv_rollback;

-- ----- Duplicate CREATE doesn't poison the slot (was 03813) -----
DROP NAMED SCALAR IF EXISTS cv_orphan;
CREATE NAMED SCALAR cv_orphan AS SELECT toUInt32(1);
CREATE NAMED SCALAR cv_orphan AS SELECT toUInt32(2); -- {serverError NAMED_SCALAR_ALREADY_EXISTS}
SELECT getNamedScalar('cv_orphan');
DROP NAMED SCALAR cv_orphan;
CREATE NAMED SCALAR cv_orphan AS SELECT toUInt32(99);
SELECT getNamedScalar('cv_orphan');
DROP NAMED SCALAR cv_orphan;
