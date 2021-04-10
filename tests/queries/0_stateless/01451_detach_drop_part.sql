DROP TABLE IF EXISTS mt;

CREATE TABLE mt (v UInt8) ENGINE = MergeTree() order by tuple();
SYSTEM STOP MERGES mt;

INSERT INTO mt VALUES (0);
INSERT INTO mt VALUES (1);
INSERT INTO mt VALUES (2);

SELECT v FROM mt ORDER BY v;

ALTER TABLE mt DETACH PART 'all_100_100_0'; -- { serverError 232 }

ALTER TABLE mt DETACH PART 'all_2_2_0';

SELECT v FROM mt ORDER BY v;

SELECT name FROM system.detached_parts WHERE table = 'mt';

ALTER TABLE mt ATTACH PART 'all_2_2_0';

SELECT v FROM mt ORDER BY v;

SELECT name FROM system.detached_parts WHERE table = 'mt';

SELECT '-- drop part --';

ALTER TABLE mt DROP PART 'all_4_4_0';

ALTER TABLE mt ATTACH PART 'all_4_4_0'; -- { serverError 233 }

SELECT v FROM mt ORDER BY v;

SELECT '-- resume merges --';
SYSTEM START MERGES mt;
OPTIMIZE TABLE mt FINAL;

SELECT v FROM mt ORDER BY v;

SELECT name FROM system.parts WHERE table = 'mt' AND active;

DROP TABLE mt;
