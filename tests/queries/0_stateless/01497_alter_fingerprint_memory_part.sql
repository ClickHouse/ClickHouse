DROP TABLE IF EXISTS mt;

CREATE TABLE mt (v UInt8) ENGINE = MergeTree() order by tuple()
    SETTINGS min_bytes_for_wide_part = '10M', min_bytes_for_compact_part = '10M';

INSERT INTO mt VALUES (0);
INSERT INTO mt VALUES (1);
INSERT INTO mt VALUES (2);

-- not supported for in memory parts
ALTER TABLE mt ADD FINGERPRINT 'my-unique-fp1' FOR PART 'all_2_2_0'; -- { serverError 48 }
ALTER TABLE mt REMOVE FINGERPRINT 'my-unique-fp1' FOR PART 'all_2_2_0'; -- { serverError 48 }

DROP TABLE mt;
