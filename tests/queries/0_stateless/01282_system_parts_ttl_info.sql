DROP TABLE IF EXISTS ttl;
CREATE TABLE ttl (d DateTime) ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 10 DAY SETTINGS remove_empty_parts=0;
SYSTEM STOP MERGES ttl;
INSERT INTO ttl VALUES ('2000-01-01 01:02:03'), ('2000-02-03 04:05:06');
SELECT rows, delete_ttl_info_min, delete_ttl_info_max, move_ttl_info.expression, move_ttl_info.min, move_ttl_info.max FROM system.parts WHERE database = currentDatabase() AND table = 'ttl';
SYSTEM START MERGES ttl;
OPTIMIZE TABLE ttl FINAL;
SELECT rows, toTimeZone(delete_ttl_info_min, 'UTC'), toTimeZone(delete_ttl_info_max, 'UTC'), move_ttl_info.expression, move_ttl_info.min, move_ttl_info.max FROM system.parts WHERE database = currentDatabase() AND table = 'ttl' AND active;
DROP TABLE ttl;
