-- 26.7 shipped TopK (`ORDER BY ... LIMIT n`) reads participating in the query condition cache
-- unconditionally, so `compatibility = '26.7'` must re-enable the gate, while the new default is off.

SELECT getSetting('use_query_condition_cache_for_top_k');
SELECT getSetting('use_query_condition_cache_for_top_k') SETTINGS compatibility = '26.7';
SELECT getSetting('use_query_condition_cache_for_top_k') SETTINGS compatibility = '26.8';
