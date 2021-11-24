-- Tags: no-unbundled

-- There are currently 594 timezones which are used from the binary embedded inside the ClickHouse binary
-- Refer: contrib/cctz-cmake/CMakeLists.txt for the complete list. The count may change but we expect there will be at least 500 timezones.
-- SELECT count(*)
-- FROM system.time_zones
--
-- ┌─count()─┐
-- │     594 │
-- └─────────┘
SELECT if ((SELECT count(*) FROM system.time_zones) > 500, 'ok', 'fail');
