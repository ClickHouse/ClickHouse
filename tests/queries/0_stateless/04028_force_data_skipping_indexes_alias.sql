SELECT name, value, changed, default
FROM system.settings
WHERE name = 'force_data_skipping_indices'
SETTINGS force_data_skipping_indexes = 'idx_a';

SELECT getSetting('force_data_skipping_indices')
SETTINGS force_data_skipping_indexes = 'idx_b';

SET force_data_skipping_indexes = 'idx_c';

SELECT name, value, changed, default
FROM system.settings
WHERE name = 'force_data_skipping_indices';

SELECT getSetting('force_data_skipping_indices');

SET force_data_skipping_indices = DEFAULT;
