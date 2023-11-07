DROP TABLE IF EXISTS part_check_t SYNC;

CREATE TABLE part_check_t(k UInt32, v String) ENGINE ReplicatedMergeTree('/{database}/part_check', 'r1') ORDER BY k SETTINGS background_part_check_delay_seconds=0;

insert into part_check_t select number, toString(number) from numbers(1000);

SELECT count() > 0
FROM system.text_log
WHERE logger_name ILIKE '%' || currentDatabase() || '%part_check%ReplicatedMergeTreePartCheckThread%' AND message ILIKE '%Background part check succeeded%' AND event_date >= yesterday()
GROUP BY logger_name;

DROP TABLE part_check_t SYNC;

CREATE TABLE no_part_check_t(k UInt32, v String) ENGINE ReplicatedMergeTree('/{database}/no_part_check', 'r1') ORDER BY k SETTINGS background_part_check_time_to_total_time_ratio=0;

insert into no_part_check_t select number, toString(number) from numbers(1000);

SELECT count()
FROM system.text_log
WHERE logger_name ILIKE '%' || currentDatabase() || '%no_part_check_t%ReplicatedMergeTreePartCheckThread%' AND message ILIKE '%Background part check%' AND event_date >= yesterday();

DROP TABLE no_part_check_t SYNC;
