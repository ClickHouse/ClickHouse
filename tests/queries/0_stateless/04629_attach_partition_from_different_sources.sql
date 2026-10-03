-- Tags: zookeeper

-- Attaching identical data from two different source tables must not be deduplicated
-- against each other (issue #105632), while a retry of the attach from the same source
-- table must still be deduplicated.

DROP TABLE IF EXISTS dst_105632 SYNC;
DROP TABLE IF EXISTS src1_105632;
DROP TABLE IF EXISTS src2_105632;

CREATE TABLE dst_105632 (k Int32, v Int32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst_105632', 'r1')
PARTITION BY k ORDER BY v;

CREATE TABLE src1_105632 (k Int32, v Int32) ENGINE = MergeTree PARTITION BY k ORDER BY v;
CREATE TABLE src2_105632 (k Int32, v Int32) ENGINE = MergeTree PARTITION BY k ORDER BY v;

-- Identical data in both source tables, so the parts have identical checksums.
INSERT INTO src1_105632 VALUES (0, 1), (0, 2), (0, 3);
INSERT INTO src2_105632 VALUES (0, 1), (0, 2), (0, 3);

ALTER TABLE dst_105632 ATTACH PARTITION 0 FROM src1_105632;
SELECT 'after attach from src1', count() FROM dst_105632;

-- Identical data from a different source table: must be attached, not deduplicated.
ALTER TABLE dst_105632 ATTACH PARTITION 0 FROM src2_105632;
SELECT 'after attach from src2', count() FROM dst_105632;

-- The same operation repeated from the same source table must still be deduplicated.
ALTER TABLE dst_105632 ATTACH PARTITION 0 FROM src1_105632;
SELECT 'after repeated attach from src1', count() FROM dst_105632;

DROP TABLE dst_105632 SYNC;
DROP TABLE src1_105632;
DROP TABLE src2_105632;
