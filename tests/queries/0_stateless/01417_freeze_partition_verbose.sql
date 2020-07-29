DROP TABLE IF EXISTS table_for_freeze;

CREATE TABLE table_for_freeze
(
  key UInt64,
  value String
)
ENGINE = MergeTree()
ORDER BY key
PARTITION BY key % 10;

INSERT INTO table_for_freeze SELECT number, toString(number) from numbers(10);

ALTER TABLE table_for_freeze FREEZE WITH NAME 'test_01417' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;

ALTER TABLE table_for_freeze FREEZE PARTITION '3' WITH NAME 'test_01417_single_part' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;

ALTER TABLE table_for_freeze DETACH PARTITION '3';

INSERT INTO table_for_freeze VALUES (3, '3');

ALTER TABLE table_for_freeze ATTACH PARTITION '3' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;

ALTER TABLE table_for_freeze DETACH PARTITION '5';

ALTER TABLE table_for_freeze FREEZE PARTITION '7' WITH NAME 'test_01417_single_part_7', ATTACH PART '5_6_6_0' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;

DROP TABLE IF EXISTS table_for_freeze;
