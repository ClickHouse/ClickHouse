-- `cascades_aggregation_pushdown` is a new default-true setting; `compatibility` with versions
-- before 26.9 must restore the pre-existing behavior (disabled).
-- This only checks the exposed setting value; `04926_cascades_aggregation_pushdown` case 1b pins
-- the resulting classic plan shape end-to-end.

SELECT '-- default: enabled';
SELECT value FROM system.settings WHERE name = 'cascades_aggregation_pushdown';

SELECT '-- compatibility = 26.8: disabled';
SELECT value FROM system.settings WHERE name = 'cascades_aggregation_pushdown' SETTINGS compatibility = '26.8';
