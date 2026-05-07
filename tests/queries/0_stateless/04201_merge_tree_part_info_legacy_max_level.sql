-- Test: exercises `tryParsePartName` LEGACY_MAX_LEVEL replacement branch
-- Covers: src/Storages/MergeTree/MergeTreePartInfo.cpp:133-141 — the
--   `if (level == LEGACY_MAX_LEVEL)` branch where `use_legacy_max_level=true`
--   is set and the level is normalized to MAX_LEVEL (999999999) so `contains`
--   and comparison operators behave correctly with such virtual parts.
-- Risk if broken: parts with the legacy max level (4294967295) would no longer
--   normalize to MAX_LEVEL, breaking `contains` checks for REPLACE/MOVE PARTITION
--   virtual drop-range parts created on/before 21.6.

SET enable_analyzer = 1;

-- Parsed level is normalized from LEGACY_MAX_LEVEL (4294967295) to MAX_LEVEL (999999999).
SELECT mergeTreePartInfo('all_0_5_4294967295').level;

-- Both LEGACY_MAX_LEVEL and MAX_LEVEL parts cover smaller-level parts in same range.
SELECT isMergeTreePartCoveredBy('all_0_3_2', 'all_0_5_4294967295');
SELECT isMergeTreePartCoveredBy('all_0_3_2', 'all_0_5_999999999');

-- A normalized legacy-max-level part does NOT cover a part outside its block range.
SELECT isMergeTreePartCoveredBy('all_6_8_0', 'all_0_5_4294967295');

-- The full info tuple — partition_id should be 'all', level normalized to 999999999.
SELECT mergeTreePartInfo('all_0_5_4294967295');
