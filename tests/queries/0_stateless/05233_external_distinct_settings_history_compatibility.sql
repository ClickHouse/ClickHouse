-- External `DISTINCT` first ships in 26.9, so `max_bytes_before_external_distinct` and
-- `max_bytes_ratio_before_external_distinct` are recorded under 26.9 in the settings-changes history.
-- `compatibility` reverts every entry of a version above the requested one, so `compatibility = '26.9'`
-- has to keep the spill enabled and only `compatibility = '26.8'` or earlier may turn it off.
-- Check the default and the two neighbouring versions.

SELECT getSetting('max_bytes_ratio_before_external_distinct');
SELECT getSetting('max_bytes_ratio_before_external_distinct') SETTINGS compatibility = '26.8';
SELECT getSetting('max_bytes_ratio_before_external_distinct') SETTINGS compatibility = '26.9';

-- The byte threshold is recorded in the same version, and it is off on both sides of the boundary.

SELECT getSetting('max_bytes_before_external_distinct');
SELECT getSetting('max_bytes_before_external_distinct') SETTINGS compatibility = '26.8';
SELECT getSetting('max_bytes_before_external_distinct') SETTINGS compatibility = '26.9';
