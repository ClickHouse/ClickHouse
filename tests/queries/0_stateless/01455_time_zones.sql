-- There are currently 562 timezones which are used from the binary embedded inside the ClickHouse binary
-- Refer: contrib/cctz-cmake/CMakeLists.txt for the complete list. The count should match when queried.
-- SELECT count(*)
-- FROM system.time_zones
--
-- ┌─count()─┐
-- │     562 │
-- └─────────┘
SELECT if ((SELECT count(*) FROM system.time_zones) = 562, 'ok', 'fail');
