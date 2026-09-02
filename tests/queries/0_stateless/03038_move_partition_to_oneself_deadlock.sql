SET optimize_trivial_insert_select = 1;

DROP TABLE IF EXISTS move_partition_to_oneself;
CREATE TABLE move_partition_to_oneself (key UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO move_partition_to_oneself SELECT number FROM numbers(1e6);
SELECT partition, rows FROM system.parts WHERE database = currentDatabase() AND table = 'move_partition_to_oneself' and active;
ALTER TABLE move_partition_to_oneself MOVE PARTITION tuple() TO TABLE move_partition_to_oneself;
SELECT partition, rows FROM system.parts WHERE database = currentDatabase() AND table = 'move_partition_to_oneself' and active;
