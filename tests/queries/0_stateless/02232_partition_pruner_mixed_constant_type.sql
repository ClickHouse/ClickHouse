-- Disable force_primary_key_reverse_order: tests partition pruning with MergeTree, output depends on key direction
SET force_primary_key_reverse_order = 0;

DROP TABLE IF EXISTS broken;

CREATE TABLE broken (time UInt64) ENGINE = MergeTree PARTITION BY toYYYYMMDD(toDate(time / 1000)) ORDER BY time;
INSERT INTO broken (time) VALUES (1647353101000), (1647353101001), (1647353101002), (1647353101003);
SELECT * FROM broken WHERE time>-1;

DROP TABLE broken;
