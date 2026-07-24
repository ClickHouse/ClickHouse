-- Tags: no-random-settings
-- no-random-settings: the settings-randomization harness sets `materialize_statistics_on_insert`
-- explicitly at the session level, and `compatibility` never overrides a setting the user has
-- already changed, so the compatibility revert this test checks would be a no-op under randomization.

-- Regression test for the `compatibility` contract of the statistics-on-insert defaults.
-- Enabling `materialize_statistics_on_insert` by default (26.8) is bounded by the new
-- `materialize_statistics_on_insert_max_table_size` setting. Before 26.8 there was no size cap,
-- so an older `compatibility` level must restore the old behavior: the flag off, and the size
-- cap at 0 (no limit) so a user who explicitly re-enables the flag still materializes statistics
-- regardless of table size.

-- New defaults (26.8).
SELECT name, value
FROM system.settings
WHERE name IN ('materialize_statistics_on_insert', 'materialize_statistics_on_insert_max_table_size')
ORDER BY name;

-- Old behavior restored by compatibility with a pre-26.8 version.
SET compatibility = '26.7';
SELECT name, value
FROM system.settings
WHERE name IN ('materialize_statistics_on_insert', 'materialize_statistics_on_insert_max_table_size')
ORDER BY name;
