-- The gate is backported to 26.7, so the settings-history entry uses `previous_value = false`:
-- no `compatibility` profile re-enables the query condition cache for TopK (`ORDER BY ... LIMIT n`)
-- reads. Check the default and the two neighbouring compatibility versions.

SELECT getSetting('use_query_condition_cache_for_top_k');
SELECT getSetting('use_query_condition_cache_for_top_k') SETTINGS compatibility = '26.7';
SELECT getSetting('use_query_condition_cache_for_top_k') SETTINGS compatibility = '26.8';
