DROP TABLE IF EXISTS mt_01451;

CREATE TABLE mt_01451 (v UInt8) ENGINE = MergeTree() order by tuple() SETTINGS old_parts_lifetime=0;
SYSTEM STOP MERGES mt_01451;

INSERT INTO mt_01451 VALUES (0);
INSERT INTO mt_01451 VALUES (1);
INSERT INTO mt_01451 VALUES (2);

SELECT v FROM mt_01451 ORDER BY v;

ALTER TABLE mt_01451 DETACH PART 'all_100_100_0'; -- { serverError NO_SUCH_DATA_PART }

ALTER TABLE mt_01451 DETACH PART 'all_2_2_0';

SELECT v FROM mt_01451 ORDER BY v;

SELECT name FROM system.detached_parts WHERE table = 'mt_01451' AND database = currentDatabase();

ALTER TABLE mt_01451 ATTACH PART 'all_2_2_0';

SELECT v FROM mt_01451 ORDER BY v;

SELECT name FROM system.detached_parts WHERE table = 'mt_01451' AND database = currentDatabase();

SELECT '-- drop part --';

ALTER TABLE mt_01451 DROP PART 'all_4_4_0';

ALTER TABLE mt_01451 ATTACH PART 'all_4_4_0'; -- { serverError BAD_DATA_PART_NAME }

SELECT v FROM mt_01451 ORDER BY v;

SELECT name FROM system.parts WHERE table = 'mt_01451' AND active AND database = currentDatabase();

SELECT '-- resume merges --';
SYSTEM START MERGES mt_01451;
OPTIMIZE TABLE mt_01451 FINAL;

SELECT v FROM mt_01451 ORDER BY v;

SELECT name FROM system.parts WHERE table = 'mt_01451' AND active AND database = currentDatabase();

DROP TABLE mt_01451;
