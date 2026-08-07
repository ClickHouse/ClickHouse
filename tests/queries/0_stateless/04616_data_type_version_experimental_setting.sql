-- Verifies Version is gated behind allow_experimental_version_type (default off), mirroring the
-- enable_time_time64_type precedent: only *declaring a column* of type Version (CREATE TABLE,
-- ALTER ADD/MODIFY COLUMN) or an explicit CAST/`::` into Version is gated. toVersion(...) itself
-- and wire-format decoding remain usable unconditionally, exactly like toTime()/toTime64().

-- 1. CREATE TABLE with an explicit Version column fails by default.
DROP TABLE IF EXISTS version_gate_test;
CREATE TABLE version_gate_test (id UInt32, ver Version) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }

-- 2. Succeeds once the setting is enabled.
SET allow_experimental_version_type = 1;
CREATE TABLE version_gate_test (id UInt32, ver Version) ENGINE = MergeTree ORDER BY id;
DROP TABLE version_gate_test;
SET allow_experimental_version_type = 0;

-- 3. CAST(...AS Version) / ::Version is gated (CastOverloadResolver.cpp call site).
SELECT CAST('1.2.3.4' AS Version); -- { serverError ILLEGAL_COLUMN }
SELECT '1.2.3.4'::Version; -- { serverError ILLEGAL_COLUMN }
SET allow_experimental_version_type = 1;
SELECT CAST('1.2.3.4' AS Version);
SELECT '1.2.3.4'::Version;
SET allow_experimental_version_type = 0;

-- 4. ALTER TABLE ADD COLUMN ... Version is gated (AlterCommands.cpp call site).
DROP TABLE IF EXISTS version_gate_alter_test;
CREATE TABLE version_gate_alter_test (id UInt32) ENGINE = MergeTree ORDER BY id;
ALTER TABLE version_gate_alter_test ADD COLUMN ver Version; -- { serverError ILLEGAL_COLUMN }
SET allow_experimental_version_type = 1;
ALTER TABLE version_gate_alter_test ADD COLUMN ver Version;
DROP TABLE version_gate_alter_test;
SET allow_experimental_version_type = 0;

-- 5. Nested usage is gated too (LowCardinality(Version), Array(Version)), via the same recursive
--    validate_callback / forEachChild mechanism.
CREATE TABLE version_gate_lc_test (id UInt32, ver LowCardinality(Version)) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE version_gate_arr_test (id UInt32, ver Array(Version)) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }

-- 6. toVersion(...) and toTypeName() on its result remain usable regardless of the setting --
--    the setting is still at its default (0) here.
SELECT throwIf(toTypeName(toVersion('1.2.3.4')) != 'Version', 'FAIL: toVersion must remain callable regardless of the gate');
