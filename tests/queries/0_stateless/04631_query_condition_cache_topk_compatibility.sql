-- Tags: no-random-settings
-- Tag no-random-settings: the test reads the effective value of
-- `use_query_condition_cache_for_top_k`, which the CI randomizer also sets. A randomized
-- value is passed on the command line, so it overrides both the default asserted on the
-- first line and the `compatibility` clauses on the next two. The test only introspects
-- settings, so it gains nothing from randomization.

-- The query condition cache for TopK (`ORDER BY ... LIMIT n`) reads is enabled by default in 26.8,
-- while 26.7 has the gate off (it was backported there). The settings-history entry uses
-- `previous_value = false`, so `compatibility` with 26.7 or earlier turns the gate back off and
-- `compatibility` with 26.8 keeps it on. Check the default and the two neighbouring versions.

SELECT getSetting('use_query_condition_cache_for_top_k');
SELECT getSetting('use_query_condition_cache_for_top_k') SETTINGS compatibility = '26.7';
SELECT getSetting('use_query_condition_cache_for_top_k') SETTINGS compatibility = '26.8';
