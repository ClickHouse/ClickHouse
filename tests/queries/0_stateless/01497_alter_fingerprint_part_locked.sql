DROP TABLE IF EXISTS mt;

CREATE TABLE mt (v UInt8) ENGINE = MergeTree() order by tuple();

SYSTEM STOP MERGES;

INSERT INTO mt VALUES (0);
INSERT INTO mt VALUES (1);
INSERT INTO mt VALUES (2);

ALTER TABLE mt UPDATE v = 1 WHERE 1 = 1;

-- this part is locked by a pending mutation
ALTER TABLE mt ADD FINGERPRINT 'my-unique-fp1' FOR PART 'all_2_2_0'; -- { serverError 384 }

SYSTEM START MERGES;
DROP TABLE mt;
