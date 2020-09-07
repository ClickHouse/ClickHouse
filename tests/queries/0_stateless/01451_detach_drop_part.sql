DROP TABLE IF EXISTS attach_01451_mt;

CREATE TABLE attach_01451_mt (v UInt8) ENGINE = MergeTree() order by tuple();

INSERT INTO attach_01451_mt VALUES (0);
INSERT INTO attach_01451_mt VALUES (1);
INSERT INTO attach_01451_mt VALUES (2);

SELECT v FROM attach_01451_mt ORDER BY v;

ALTER TABLE attach_01451_mt DETACH PART 'all_2_2_0';

SELECT v FROM attach_01451_mt ORDER BY v;

SELECT name FROM system.detached_parts WHERE table = 'attach_01451_mt';

ALTER TABLE attach_01451_mt ATTACH PART 'all_2_2_0';

SELECT v FROM attach_01451_mt ORDER BY v;

SELECT name FROM system.detached_parts WHERE table = 'attach_01451_mt';

SELECT '-- drop part --';

ALTER TABLE attach_01451_mt DROP PART 'all_4_4_0';

ALTER TABLE attach_01451_mt ATTACH PART 'all_4_4_0'; -- { serverError 233 }

SELECT v FROM attach_01451_mt ORDER BY v;

DROP TABLE attach_01451_mt;
