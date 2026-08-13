-- Tags: no-replicated-database
-- Tag no-replicated-database: Unsupported type of ALTER query

DROP TABLE IF EXISTS merge_tree_deduplication;

CREATE TABLE merge_tree_deduplication
(
    key UInt64,
    value String,
    part UInt8 DEFAULT 77
)
ENGINE=MergeTree()
ORDER BY key
PARTITION BY part
SETTINGS non_replicated_deduplication_window=3;

SYSTEM STOP MERGES merge_tree_deduplication;

INSERT INTO merge_tree_deduplication (key, value) VALUES (1, '1');

SELECT key, value FROM merge_tree_deduplication;

INSERT INTO merge_tree_deduplication (key, value) VALUES (1, '1');

SELECT key, value FROM merge_tree_deduplication;

SELECT '===============';

INSERT INTO merge_tree_deduplication (key, value) VALUES (2, '2');

INSERT INTO merge_tree_deduplication (key, value) VALUES (3, '3');

INSERT INTO merge_tree_deduplication (key, value) VALUES (4, '4');

INSERT INTO merge_tree_deduplication (key, value) VALUES (1, '1');

SELECT key, value FROM merge_tree_deduplication ORDER BY key;

SELECT '===============';

INSERT INTO merge_tree_deduplication (key, value) VALUES (5, '5');

INSERT INTO merge_tree_deduplication (key, value) VALUES (6, '6');

INSERT INTO merge_tree_deduplication (key, value) VALUES (7, '7');

INSERT INTO merge_tree_deduplication (key, value) VALUES (5, '5');

SELECT key, value FROM merge_tree_deduplication ORDER BY key;

SELECT '===============';

INSERT INTO merge_tree_deduplication (key, value) VALUES (8, '8');

INSERT INTO merge_tree_deduplication (key, value) VALUES (9, '9');

INSERT INTO merge_tree_deduplication (key, value) VALUES (10, '10');

INSERT INTO merge_tree_deduplication (key, value) VALUES (11, '11');

INSERT INTO merge_tree_deduplication (key, value) VALUES (12, '12');

INSERT INTO merge_tree_deduplication (key, value) VALUES (10, '10');
INSERT INTO merge_tree_deduplication (key, value) VALUES (11, '11');
INSERT INTO merge_tree_deduplication (key, value) VALUES (12, '12');

SELECT key, value FROM merge_tree_deduplication ORDER BY key;

SELECT '===============';

ALTER TABLE merge_tree_deduplication DROP PART '77_9_9_0'; -- some old part

INSERT INTO merge_tree_deduplication (key, value) VALUES (10, '10');

SELECT key, value FROM merge_tree_deduplication WHERE key = 10;

ALTER TABLE merge_tree_deduplication DROP PART '77_13_13_0'; -- fresh part

INSERT INTO merge_tree_deduplication (key, value) VALUES (12, '12');

SELECT key, value FROM merge_tree_deduplication WHERE key = 12;

DETACH TABLE merge_tree_deduplication;
ATTACH TABLE merge_tree_deduplication;

OPTIMIZE TABLE  merge_tree_deduplication FINAL;

INSERT INTO merge_tree_deduplication (key, value) VALUES (11, '11'); -- deduplicated
INSERT INTO merge_tree_deduplication (key, value) VALUES (12, '12'); -- deduplicated

SELECT '===============';

SELECT key, value FROM merge_tree_deduplication ORDER BY key;

SELECT '===============';

INSERT INTO merge_tree_deduplication (key, value, part) VALUES (11, '11', 88);

ALTER TABLE merge_tree_deduplication DROP PARTITION 77;

INSERT INTO merge_tree_deduplication (key, value, part) VALUES (11, '11', 88); --deduplicated

INSERT INTO merge_tree_deduplication (key, value) VALUES (11, '11'); -- not deduplicated
INSERT INTO merge_tree_deduplication (key, value) VALUES (12, '12'); -- not deduplicated

SELECT part, key, value FROM merge_tree_deduplication ORDER BY key, part;

-- Alters....

ALTER TABLE merge_tree_deduplication MODIFY SETTING non_replicated_deduplication_window = 2;

SELECT '===============';

INSERT INTO merge_tree_deduplication (key, value, part) VALUES (1, '1', 33);
INSERT INTO merge_tree_deduplication (key, value, part) VALUES (2, '2', 33);
INSERT INTO merge_tree_deduplication (key, value, part) VALUES (3, '3', 33);
INSERT INTO merge_tree_deduplication (key, value, part) VALUES (1, '1', 33);

SELECT * FROM merge_tree_deduplication WHERE part = 33 ORDER BY key;

SELECT '===============';

ALTER TABLE merge_tree_deduplication MODIFY SETTING non_replicated_deduplication_window = 0;

INSERT INTO merge_tree_deduplication (key, value, part) VALUES (1, '1', 33);
INSERT INTO merge_tree_deduplication (key, value, part) VALUES (1, '1', 33);

DETACH TABLE merge_tree_deduplication;
ATTACH TABLE merge_tree_deduplication;

SELECT * FROM merge_tree_deduplication WHERE part = 33 ORDER BY key;

SELECT '===============';

ALTER TABLE merge_tree_deduplication MODIFY SETTING non_replicated_deduplication_window = 3;

INSERT INTO merge_tree_deduplication (key, value, part) VALUES (1, '1', 33);

SELECT * FROM merge_tree_deduplication WHERE part = 33 ORDER BY key;

SELECT '===============';

INSERT INTO merge_tree_deduplication (key, value, part) VALUES (1, '1', 44);
INSERT INTO merge_tree_deduplication (key, value, part) VALUES (2, '2', 44);
INSERT INTO merge_tree_deduplication (key, value, part) VALUES (3, '3', 44);
INSERT INTO merge_tree_deduplication (key, value, part) VALUES (1, '1', 44);

INSERT INTO merge_tree_deduplication (key, value, part) VALUES (4, '4', 44);

DETACH TABLE merge_tree_deduplication;
ATTACH TABLE merge_tree_deduplication;

SELECT * FROM merge_tree_deduplication WHERE part = 44 ORDER BY key;

DROP TABLE IF EXISTS merge_tree_deduplication;

SELECT '===============';

DROP TABLE IF EXISTS merge_tree_no_deduplication;

CREATE TABLE merge_tree_no_deduplication
(
    key UInt64,
    value String
)
ENGINE=MergeTree()
ORDER BY key;

INSERT INTO merge_tree_no_deduplication (key, value) VALUES (1, '1');
INSERT INTO merge_tree_no_deduplication (key, value) VALUES (1, '1');

SELECT * FROM merge_tree_no_deduplication ORDER BY key;

SELECT '===============';

ALTER TABLE merge_tree_no_deduplication MODIFY SETTING non_replicated_deduplication_window = 3;

INSERT INTO merge_tree_no_deduplication (key, value) VALUES (1, '1');
INSERT INTO merge_tree_no_deduplication (key, value) VALUES (2, '2');
INSERT INTO merge_tree_no_deduplication (key, value) VALUES (3, '3');

DETACH TABLE merge_tree_no_deduplication;
ATTACH TABLE merge_tree_no_deduplication;

INSERT INTO merge_tree_no_deduplication (key, value) VALUES (1, '1');
INSERT INTO merge_tree_no_deduplication (key, value) VALUES (4, '4');

SELECT * FROM merge_tree_no_deduplication ORDER BY key;

DROP TABLE IF EXISTS merge_tree_no_deduplication;
