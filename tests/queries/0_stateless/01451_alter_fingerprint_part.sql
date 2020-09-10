DROP TABLE IF EXISTS mt;

CREATE TABLE mt (v UInt8) ENGINE = MergeTree() order by tuple();
SYSTEM STOP MERGES;

INSERT INTO mt VALUES (0);
INSERT INTO mt VALUES (1);
INSERT INTO mt VALUES (2);

ALTER TABLE mt ADD FINGERPRINT FOR PART 'all_2_2_0'; -- { serverError 48 }

SELECT '-- resume merges --';
SYSTEM START MERGES;
OPTIMIZE TABLE mt FINAL;

SELECT name, length(fingerprint) FROM system.parts WHERE table = 'mt' AND active;

SELECT '-- detach / attach --';

DETACH TABLE mt;
ATTACH TABLE mt;
OPTIMIZE TABLE mt FINAL;

SELECT name, length(fingerprint) FROM system.parts WHERE table = 'mt' AND active;

SELECT '-- remove fingerprint --';

ALTER TABLE mt REMOVE FINGERPRINT FOR PART 'all_2_2_0'; -- { serverError 48 }
OPTIMIZE TABLE mt FINAL;
SELECT name, length(fingerprint) FROM system.parts WHERE table = 'mt' AND active;

DROP TABLE mt;
