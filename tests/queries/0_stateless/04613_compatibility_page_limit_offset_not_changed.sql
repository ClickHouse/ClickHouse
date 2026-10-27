-- `page`, `limit`, and `offset` are `Double` settings, and their entries in `SettingsChangesHistory`
-- must record `Float64` values. With integer values, the type-strict `Field` comparison in
-- `applyCompatibilitySetting` considers `Float64(0) != UInt64(0)` and marks all three settings as
-- changed even at the default value, leaking them onto the native wire to older peers.
SET compatibility = '26.6';
SELECT name FROM system.settings WHERE name IN ('page', 'limit', 'offset') AND changed;
