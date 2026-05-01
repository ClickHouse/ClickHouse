DROP TABLE IF EXISTS users;
SET session_timezone = 'UTC';

CREATE TABLE users (uid Int16, d DateTime('UTC'))
ENGINE = MergeTree ORDER BY uid TTL d + INTERVAL 1 MONTH WHERE uid = 1
SETTINGS merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 0, vertical_merge_algorithm_min_rows_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 0;

SYSTEM STOP TTL MERGES users;

INSERT INTO users SELECT arrayJoin([1,2]), toDateTime('2020-01-01 00:00:00', 'UTC');
INSERT INTO users SELECT arrayJoin([2,3]), toDateTime('2020-01-01 00:00:00', 'UTC');

SELECT * FROM users ORDER BY ALL;

SELECT
    delete_ttl_info_min,
    delete_ttl_info_max,
    rows_where_ttl_info.min,
    rows_where_ttl_info.max
FROM system.parts WHERE database = currentDatabase() AND table = 'users' AND active
ORDER BY name;

SYSTEM START TTL MERGES users;
OPTIMIZE TABLE users FINAL;

SELECT * FROM users ORDER BY ALL;

SELECT
    delete_ttl_info_min,
    delete_ttl_info_max,
    rows_where_ttl_info.min,
    rows_where_ttl_info.max
FROM system.parts WHERE database = currentDatabase() AND table = 'users' AND active
ORDER BY name;

-- Cannot assign merge because there is one part and all
-- TTLs that should be applied are already applied.
-- Previously it would succeed for TTL with WHERE.
OPTIMIZE TABLE users SETTINGS optimize_throw_if_noop = 1; -- { serverError CANNOT_ASSIGN_OPTIMIZE }

DETACH TABLE users;
ATTACH TABLE users;

-- Check that expired TTL doesn't affect the vertical merge algorithm
OPTIMIZE TABLE users FINAL;
SYSTEM FLUSH LOGS part_log;
SELECT merge_algorithm FROM system.part_log WHERE database = currentDatabase() AND table = 'users' AND event_type = 'MergeParts' ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE IF EXISTS users;
