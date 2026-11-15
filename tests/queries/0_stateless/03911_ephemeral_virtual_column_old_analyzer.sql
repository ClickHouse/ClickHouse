-- Ephemeral common virtual columns (like _table) should not be resolved
-- by the old analyzer, which has no mechanism to fill them.
-- This used to cause a logical error due to type mismatch.

SELECT _table SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_IDENTIFIER }
SELECT _table FROM system.one SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_IDENTIFIER }

-- With the analyzer, _table should work fine.
SELECT _table SETTINGS enable_analyzer = 1;
SELECT _table FROM system.one SETTINGS enable_analyzer = 1;
