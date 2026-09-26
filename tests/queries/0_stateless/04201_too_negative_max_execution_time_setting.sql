-- Test: exercises `float64AsSecondsToTimespan` with a very negative value triggering the
--       `d * 1000000 < std::numeric_limits<Poco::Timespan::TimeDiff>::min()` branch.
-- Covers: src/Core/SettingsFields.cpp:320 — negative-overflow half of the new range check.
-- The PR-shipped test (03000_too_big_max_execution_time_setting.sql) only covers the
-- positive-overflow half (`d * 1000000 > max()`); the negative-overflow half is the actual
-- UBSan trigger from the linked failure report (-9.22337e+24) but has no test exercising it.
SELECT 1 SETTINGS max_execution_time = -1e18; -- { clientError BAD_ARGUMENTS }
