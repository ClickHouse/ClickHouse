DROP TABLE IF EXISTS mt;

-- force wide part, non-polymorphic
CREATE TABLE mt (v UInt8) ENGINE = MergeTree() order by tuple()
    SETTINGS min_bytes_for_compact_part = 0, min_rows_for_compact_part = 0,
        min_bytes_for_wide_part = '10M', min_rows_for_wide_part = 10;

-- can't stop merges as they stop mutations as well :/
-- SYSTEM STOP MERGES;

INSERT INTO mt VALUES (0);
INSERT INTO mt VALUES (1);
INSERT INTO mt VALUES (2);

-- this part does not exist
ALTER TABLE mt ADD FINGERPRINT 'my-unique-fp1' FOR PART 'all_2_42_8'; -- { serverError 257 }

ALTER TABLE mt ADD FINGERPRINT 'my-unique-fp1' FOR PART 'all_2_2_0';

SELECT '-- resume merges --';
SYSTEM START MERGES;
OPTIMIZE TABLE mt FINAL;

SELECT name, fingerprint, active FROM system.parts WHERE table = 'mt' AND active;

SELECT '-- detach / attach --';

DETACH TABLE mt;
ATTACH TABLE mt;
OPTIMIZE TABLE mt FINAL;

SELECT name, fingerprint, active FROM system.parts WHERE table = 'mt' AND active;

SELECT '-- remove fingerprint --';

ALTER TABLE mt REMOVE FINGERPRINT 'my-unique-fp1' FOR PART 'all_2_2_0_4';
OPTIMIZE TABLE mt FINAL;
SELECT name, fingerprint, active FROM system.parts WHERE table = 'mt' AND active;

DROP TABLE mt;
