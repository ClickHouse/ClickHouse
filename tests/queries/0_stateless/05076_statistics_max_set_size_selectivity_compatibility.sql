-- `statistics_max_set_size_for_exact_selectivity_estimation` bounds the cost of estimating the selectivity of `IN`.
-- Its entry in the settings changes history has the previous value equal to the new one on purpose,
-- so that `compatibility` with an older version does not bring back the uncapped estimation.

SELECT value FROM system.settings WHERE name = 'statistics_max_set_size_for_exact_selectivity_estimation';

SELECT value FROM system.settings WHERE name = 'statistics_max_set_size_for_exact_selectivity_estimation'
SETTINGS compatibility = '26.6';

SELECT value FROM system.settings WHERE name = 'statistics_max_set_size_for_exact_selectivity_estimation'
SETTINGS compatibility = '25.8';

-- An explicit value still wins over `compatibility`.
SELECT value FROM system.settings WHERE name = 'statistics_max_set_size_for_exact_selectivity_estimation'
SETTINGS compatibility = '25.8', statistics_max_set_size_for_exact_selectivity_estimation = 0;
