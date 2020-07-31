DROP TABLE IF EXISTS table_for_freeze_replicated;

CREATE TABLE table_for_freeze_replicated
(
  key UInt64,
  value String
)
ENGINE = ReplicatedMergeTree('/test/table_for_freeze_replicated', '1')
ORDER BY key
PARTITION BY key % 10;

INSERT INTO table_for_freeze_replicated SELECT number, toString(number) from numbers(10);

ALTER TABLE table_for_freeze_replicated FREEZE WITH NAME 'test_01417' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;

ALTER TABLE table_for_freeze_replicated FREEZE PARTITION '3' WITH NAME 'test_01417_single_part' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;

ALTER TABLE table_for_freeze_replicated DETACH PARTITION '3';

INSERT INTO table_for_freeze_replicated VALUES (3, '3');

ALTER TABLE table_for_freeze_replicated ATTACH PARTITION '3' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;

ALTER TABLE table_for_freeze_replicated DETACH PARTITION '5';

ALTER TABLE table_for_freeze_replicated FREEZE PARTITION '7' WITH NAME 'test_01417_single_part_7', ATTACH PART '5_0_0_0' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;

DROP TABLE IF EXISTS table_for_freeze_replicated;
