-- Some engines read the `SETTINGS` clause of their definition directly instead of applying it to a
-- settings schema, so they have to reject the value-less form `SETTINGS name` themselves: it stands
-- for `name = true`, and without the check a numeric setting would silently become `1` and a String
-- one would fail from `safeGet` at the wrong layer.

DROP TABLE IF EXISTS t_04664;

-- `Join` reads its settings one by one from the storage definition.
CREATE TABLE t_04664 (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS max_rows_in_join; -- { error TYPE_MISMATCH }
CREATE TABLE t_04664 (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS disk; -- { error TYPE_MISMATCH }

-- The Bool ones are what the shorthand is for.
CREATE TABLE t_04664 (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS join_use_nulls;
SELECT extract(engine_full, 'SETTINGS.*') FROM system.tables WHERE database = currentDatabase() AND name = 't_04664';
DROP TABLE t_04664;

-- The Log family reads `disk` and `storage_policy` from the definition and has no other settings.
CREATE TABLE t_04664 (k UInt64) ENGINE = Log SETTINGS disk; -- { error TYPE_MISMATCH }
CREATE TABLE t_04664 (k UInt64) ENGINE = StripeLog SETTINGS storage_policy; -- { error TYPE_MISMATCH }
