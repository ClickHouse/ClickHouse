DROP TABLE IF EXISTS table_with_gap;

CREATE TABLE table_with_gap (v UInt8) ENGINE = MergeTree() ORDER BY tuple() settings old_parts_lifetime = 10000;
SYSTEM STOP MERGES table_with_gap;

INSERT INTO table_with_gap VALUES (0);
INSERT INTO table_with_gap VALUES (1);
INSERT INTO table_with_gap VALUES (2);

SELECT 'initial parts';
SELECT name FROM system.parts WHERE table = 'table_with_gap' AND active AND database = currentDatabase();

ALTER TABLE table_with_gap DROP PART 'all_2_2_0';

SELECT 'parts with gap';
SELECT name, active FROM system.parts WHERE table = 'table_with_gap' AND database = currentDatabase();

SYSTEM START MERGES table_with_gap;

OPTIMIZE TABLE table_with_gap FINAL;

SELECT 'parts after optimize';
SELECT name, active FROM system.parts WHERE table = 'table_with_gap' AND database = currentDatabase();

DETACH TABLE table_with_gap;
ATTACH TABLE table_with_gap;
