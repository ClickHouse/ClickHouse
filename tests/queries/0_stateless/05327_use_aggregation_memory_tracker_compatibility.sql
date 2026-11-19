-- https://github.com/ClickHouse/ClickHouse/issues/123756: `compatibility` below 26.9 restores the whole-query memory measurement of the `GROUP BY` two-level decision.

-- Reset (randomized in CI): an explicit value would hide what `compatibility` sets.
SET use_aggregation_memory_tracker = DEFAULT;

SELECT 'default', value FROM system.settings WHERE name = 'use_aggregation_memory_tracker';

SET compatibility = '26.9';
SELECT '26.9', value FROM system.settings WHERE name = 'use_aggregation_memory_tracker';

SET compatibility = '26.8';
SELECT '26.8', value FROM system.settings WHERE name = 'use_aggregation_memory_tracker';

SET compatibility = '26.7';
SELECT '26.7', value FROM system.settings WHERE name = 'use_aggregation_memory_tracker';

SET compatibility = '26.6';
SELECT '26.6', value FROM system.settings WHERE name = 'use_aggregation_memory_tracker';
