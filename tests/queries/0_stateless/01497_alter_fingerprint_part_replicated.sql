DROP TABLE IF EXISTS mt;

-- force wide part, non-polymorphic
CREATE TABLE mt (v UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_01497/mt', '1') order by tuple()
    SETTINGS index_granularity_bytes = 1024;

-- can't stop merges as they stop mutations as well :/
-- SYSTEM STOP MERGES;

INSERT INTO mt VALUES (0);
INSERT INTO mt VALUES (1);
INSERT INTO mt VALUES (2);

-- this part does not exist
ALTER TABLE mt ADD FINGERPRINT 'my-unique-fp1' FOR PART 'all_2_42_8'; -- { serverError 257 }

ALTER TABLE mt ADD FINGERPRINT 'my-unique-fp1' FOR PART 'all_1_1_0';

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

ALTER TABLE mt REMOVE FINGERPRINT 'my-unique-fp1' FOR PART 'all_1_1_0_3';
OPTIMIZE TABLE mt FINAL;
SELECT name, fingerprint, active FROM system.parts WHERE table = 'mt' AND active;

DROP TABLE mt;
