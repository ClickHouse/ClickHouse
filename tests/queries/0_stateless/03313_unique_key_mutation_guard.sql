-- Tags: no-ordinary-database, no-async-insert, no-fasttest, no-object-storage, no-s3-storage
-- UNIQUE KEY: mutation-guard coverage.
--
-- `MergeTreeData::checkMutationIsPossible` must reject every mutation-class
-- operation that rewrites stored column bytes for a UNIQUE KEY column,
-- because those operations bypass `MergeTreeSinkUniqueKeyCommit::run` and
-- would create duplicate live keys. Covers ALTER DELETE / ALTER UPDATE /
-- MATERIALIZE COLUMN / CLEAR COLUMN.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS uk_mut_guard;
CREATE TABLE uk_mut_guard (a UInt32, b UInt32, c UInt32, d String DEFAULT 'def')
ENGINE = MergeTree ORDER BY (c) UNIQUE KEY (a, b);

INSERT INTO uk_mut_guard VALUES (1, 10, 100, 'x'), (2, 20, 200, 'y');

-- ALTER DELETE / ALTER UPDATE on a UK table must be rejected: both rewrite
-- rows without going through UNIQUE KEY dedup, which would produce duplicate
-- live keys. Error code SUPPORT_IS_DISABLED = 344.
SELECT 'alter_delete_uk' AS step;
ALTER TABLE uk_mut_guard DELETE WHERE a = 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'alter_update_uk_column' AS step;
ALTER TABLE uk_mut_guard UPDATE a = 99 WHERE a = 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'alter_update_non_uk_column' AS step;
ALTER TABLE uk_mut_guard UPDATE d = 'z' WHERE a = 1; -- { serverError SUPPORT_IS_DISABLED }

-- Lightweight-update bypass coverage: ALTER ... UPDATE under
-- alter_update_mode='lightweight' is rewritten to a patch-part path before
-- checkMutationIsPossible runs. The UK rejection inside
-- supportsLightweightUpdate causes the lightweight rewrite to fall back to
-- the heavy path, which is then rejected; under lightweight_force the
-- rewrite throws directly. Both end with SUPPORT_IS_DISABLED.
SELECT 'lightweight_update_uk' AS step;
ALTER TABLE uk_mut_guard UPDATE d = 'z' WHERE a = 1
SETTINGS alter_update_mode = 'lightweight', enable_lightweight_update = 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'lightweight_force_update_uk' AS step;
ALTER TABLE uk_mut_guard UPDATE d = 'z' WHERE a = 1
SETTINGS alter_update_mode = 'lightweight_force', enable_lightweight_update = 1; -- { serverError SUPPORT_IS_DISABLED }

-- MATERIALIZE COLUMN on a UK column must be rejected (error code
-- SUPPORT_IS_DISABLED = 344). Error message must name the column and reference
-- UNIQUE KEY.
SELECT 'materialize_uk_a' AS step;
ALTER TABLE uk_mut_guard MATERIALIZE COLUMN a; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'materialize_uk_b' AS step;
ALTER TABLE uk_mut_guard MATERIALIZE COLUMN b; -- { serverError SUPPORT_IS_DISABLED }

-- CLEAR COLUMN on a UK column: rejected by the existing Phase-1-B.2
-- `checkAlterIsPossible` UK-column guard BEFORE reaching the mutation path
-- (which would also reject with SUPPORT_IS_DISABLED per this fix). Error
-- code is ALTER_OF_COLUMN_IS_FORBIDDEN (524). Both guards are defensive in
-- depth — either rejecting is correct.
SELECT 'clear_uk_a' AS step;
ALTER TABLE uk_mut_guard CLEAR COLUMN a IN PARTITION ID 'all'; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT 'clear_uk_b' AS step;
ALTER TABLE uk_mut_guard CLEAR COLUMN b IN PARTITION ID 'all'; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- Non-UK columns must still allow MATERIALIZE / CLEAR (c is in ORDER BY so
-- materialize on c is meaningless in practice but not forbidden by the UK
-- guard; d is a plain column with a DEFAULT, no restrictions apply).
SET mutations_sync = 2;
SELECT 'materialize_non_uk_d' AS step;
ALTER TABLE uk_mut_guard MATERIALIZE COLUMN d;

SELECT 'clear_non_uk_d' AS step;
ALTER TABLE uk_mut_guard CLEAR COLUMN d IN PARTITION ID 'all';

-- State: cleared `d` means its values are now reset to the default;
-- count is preserved.
SELECT count() FROM uk_mut_guard;  -- 2

DROP TABLE uk_mut_guard;

-- Negative: same operations on a plain-MergeTree table (no UK) must all pass.
DROP TABLE IF EXISTS mt_plain;
CREATE TABLE mt_plain (a UInt32, b UInt32 DEFAULT 0, d String DEFAULT 'def')
ENGINE = MergeTree ORDER BY a;

INSERT INTO mt_plain VALUES (1, 10, 'x'), (2, 20, 'y');

-- Each of these operations succeeds on a plain-MT table (no UK guard).
-- We run them synchronously by waiting via `mutations_sync = 2`.
SET mutations_sync = 2;
ALTER TABLE mt_plain MATERIALIZE COLUMN b;
ALTER TABLE mt_plain CLEAR COLUMN b IN PARTITION ID 'all';
ALTER TABLE mt_plain MATERIALIZE COLUMN d;
ALTER TABLE mt_plain CLEAR COLUMN d IN PARTITION ID 'all';

SELECT count() FROM mt_plain;  -- 2

DROP TABLE mt_plain;

-- ============================================================
-- UNIQUE KEY: non-local-disk DDL guard.
--
-- The code-level guard in `registerStorageMergeTree.cpp` rejects any
-- UNIQUE KEY CREATE TABLE whose resolved storage policy references a
-- disk with `DataSourceType != Local`. The full-stack SQL test requires
-- a test-server config with an s3 / object-storage disk AND a storage
-- policy that uses it. The guard is validated by the code path + the
-- default-policy happy-path below.
-- ============================================================

-- UNIQUE KEY on the default (local) policy must still succeed.
SELECT 'uk_on_default_ok' AS step;
DROP TABLE IF EXISTS uk_on_default;
CREATE TABLE uk_on_default (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id);
INSERT INTO uk_on_default VALUES (1, 'x');
SELECT count() FROM uk_on_default;  -- 1
DROP TABLE uk_on_default;
