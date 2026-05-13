-- Tags: no-parallel
SET allow_experimental_named_scalars = 1;

DROP NAMED SCALAR IF EXISTS cv_kind_local;

-- ----- SHARED requires Keeper config; without it a clear error fires
--       before any other validation (was 03804 + 03806).
CREATE SHARED NAMED SCALAR foo AS SELECT 1; -- {serverError SHARED_NAMED_SCALARS_NOT_CONFIGURED}
CREATE OR REPLACE SHARED NAMED SCALAR foo AS SELECT toUInt64(5); -- {serverError SHARED_NAMED_SCALARS_NOT_CONFIGURED}
CREATE SHARED NAMED SCALAR IF NOT EXISTS foo REFRESH EVERY 1 MINUTE AS SELECT now(); -- {serverError SHARED_NAMED_SCALARS_NOT_CONFIGURED}

-- ----- Grammar rejections (was 03804) -----
DROP SHARED NAMED SCALAR foo; -- {clientError SYNTAX_ERROR}
DROP SHARED NAMED SCALAR IF EXISTS foo; -- {clientError SYNTAX_ERROR}
DROP NAMED SCALAR foo; -- {serverError NAMED_SCALAR_NOT_FOUND}
DROP NAMED SCALAR IF EXISTS foo;

-- ON CLUSTER + SHARED is rejected by the interpreter (Keeper distributes already).
CREATE SHARED NAMED SCALAR foo ON CLUSTER x AS SELECT 1; -- {serverError SYNTAX_ERROR}
DROP SHARED NAMED SCALAR foo ON CLUSTER x; -- {clientError SYNTAX_ERROR}

-- TEMPORARY is not supported.
CREATE TEMPORARY NAMED SCALAR t AS SELECT 1; -- {clientError SYNTAX_ERROR}

-- Bare identifier required for scalar names; no compound names.
CREATE NAMED SCALAR foo.bar AS SELECT 1; -- {clientError SYNTAX_ERROR}
DROP NAMED SCALAR foo.bar; -- {clientError SYNTAX_ERROR}

-- LOCAL is accepted as an explicit cache-kind modifier.
CREATE LOCAL NAMED SCALAR local_foo AS SELECT 1;
DROP NAMED SCALAR local_foo;

-- ----- SYSTEM commands for unknown name (was 03806) -----
SYSTEM REFRESH NAMED SCALAR never_existed; -- {serverError NAMED_SCALAR_NOT_FOUND}
SYSTEM STOP NAMED SCALAR REFRESHES never_existed; -- {serverError NAMED_SCALAR_NOT_FOUND}
SYSTEM START NAMED SCALAR REFRESHES never_existed; -- {serverError NAMED_SCALAR_NOT_FOUND}
SYSTEM REFRESH TEMPORARY NAMED SCALAR anything; -- {clientError SYNTAX_ERROR}

-- ----- OR REPLACE that changes cache kind is rejected (was 03812) -----
-- Without Keeper, ensureCreatable fires before the kind-change check, so both
-- BAD_ARGUMENTS and SHARED_NAMED_SCALARS_NOT_CONFIGURED are acceptable here.
-- Integration tests cover the BAD_ARGUMENTS path with Keeper configured.
CREATE LOCAL NAMED SCALAR cv_kind_local AS SELECT toUInt32(1);
CREATE OR REPLACE SHARED NAMED SCALAR cv_kind_local AS SELECT toUInt32(2); -- {serverError BAD_ARGUMENTS,SHARED_NAMED_SCALARS_NOT_CONFIGURED}
SELECT getNamedScalar('cv_kind_local');
SELECT toTypeName(getNamedScalar('cv_kind_local'));
DROP NAMED SCALAR cv_kind_local;
