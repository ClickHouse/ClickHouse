-- Regression test for the `compatibility` contract of the statistics-on-insert defaults.
-- Enabling `materialize_statistics_on_insert` by default (26.7) is bounded by the new
-- `materialize_statistics_on_insert_max_table_size` setting. Before 26.7 there was no size cap,
-- so an older `compatibility` level must restore the old behavior: the flag off, and the size
-- cap at 0 (no limit) so a user who explicitly re-enables the flag still materializes statistics
-- regardless of table size.

-- New defaults (26.7).
SELECT name, value
FROM system.settings
WHERE name IN ('materialize_statistics_on_insert', 'materialize_statistics_on_insert_max_table_size')
ORDER BY name;

-- Old behavior restored by compatibility with a pre-26.7 version.
SET compatibility = '26.6';
SELECT name, value
FROM system.settings
WHERE name IN ('materialize_statistics_on_insert', 'materialize_statistics_on_insert_max_table_size')
ORDER BY name;
