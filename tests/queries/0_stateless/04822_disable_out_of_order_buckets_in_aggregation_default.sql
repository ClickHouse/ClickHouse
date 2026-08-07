-- Verify the default of `enable_producing_buckets_out_of_order_in_aggregation` after it was flipped to 0 by default,
-- because the initiator may lose track of buckets received out of order and produce incorrect results.

-- By default the setting is disabled.
SELECT 'Default';
SELECT value, changed FROM system.settings WHERE name = 'enable_producing_buckets_out_of_order_in_aggregation';

-- An old compatibility version restores the previous default (enabled),
-- because the default change is registered in SettingsChangesHistory under version 26.8.
SET compatibility = '26.7';
SELECT 'compatibility = 26.7';
SELECT value FROM system.settings WHERE name = 'enable_producing_buckets_out_of_order_in_aggregation';
