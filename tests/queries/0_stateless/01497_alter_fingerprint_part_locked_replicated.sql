DROP TABLE IF EXISTS mt;

CREATE TABLE mt (v UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_01497/mt_l', '1') order by tuple();

SYSTEM STOP MERGES;

INSERT INTO mt VALUES (0);
INSERT INTO mt VALUES (1);
INSERT INTO mt VALUES (2);

ALTER TABLE mt UPDATE v = 1 WHERE 1 = 1;

-- this part is locked by a pending mutation
ALTER TABLE mt ADD FINGERPRINT 'my-unique-fp1' FOR PART 'all_1_1_0' SETTINGS mutations_sync = 2; -- { serverError 384 }

SYSTEM START MERGES;
DROP TABLE mt;
