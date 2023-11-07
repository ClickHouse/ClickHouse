DROP TABLE IF EXISTS part_check_t1 SYNC;
DROP TABLE IF EXISTS part_check_t2 SYNC;
DROP TABLE IF EXISTS part_check_t3 SYNC;

CREATE TABLE part_check_t1(k UInt32, v String) ENGINE ReplicatedMergeTree('/part_check/{database}/test_tbl', 'r1') ORDER BY k SETTINGS background_part_check_delay_seconds=0;
CREATE TABLE part_check_t2(k UInt32, v String) ENGINE ReplicatedMergeTree('/part_check/{database}/test_tbl', 'r2') ORDER BY k SETTINGS background_part_check_delay_seconds=0;
CREATE TABLE part_check_t3(k UInt32, v String) ENGINE ReplicatedMergeTree('/part_check/{database}/test_tbl', 'r3') ORDER BY k SETTINGS background_part_check_delay_seconds=0;

insert into part_check_t1 select number, toString(number) from numbers(1000, 1000);

SYSTEM SYNC REPLICA part_check_t1;
SYSTEM SYNC REPLICA part_check_t2;
SYSTEM SYNC REPLICA part_check_t3;

SELECT count() > 0
FROM system.text_log
WHERE logger_name ILIKE '%' || currentDatabase() || '%part_check_t%ReplicatedMergeTreePartCheckThread%' AND message ILIKE '%Background part check succeeded%' AND event_date >= yesterday()
GROUP BY logger_name;

DROP TABLE part_check_t1 SYNC;
DROP TABLE part_check_t2 SYNC;
DROP TABLE part_check_t3 SYNC;
