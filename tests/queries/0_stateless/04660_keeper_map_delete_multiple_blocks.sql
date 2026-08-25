-- Tags: no-ordinary-database, no-fasttest

DROP TABLE IF EXISTS 04660_keeper_map_delete SYNC;

CREATE TABLE 04660_keeper_map_delete (key UInt64, value UInt64) ENGINE = KeeperMap('/' || currentDatabase() || '/test04660_keeper_map_delete') PRIMARY KEY key;

INSERT INTO 04660_keeper_map_delete SELECT number, number FROM numbers(1000);
SELECT count() FROM 04660_keeper_map_delete;

-- A small max_block_size makes the mutation pipeline produce multiple blocks.
-- Previously only the first block of matched rows was deleted, and the rest were silently kept.
ALTER TABLE 04660_keeper_map_delete DELETE WHERE key < 750 SETTINGS max_block_size = 100;
SELECT count() FROM 04660_keeper_map_delete;

DELETE FROM 04660_keeper_map_delete WHERE 1 SETTINGS max_block_size = 100;
SELECT count() FROM 04660_keeper_map_delete;

-- The same in strict mode.
SET keeper_map_strict_mode = 1;

INSERT INTO 04660_keeper_map_delete SELECT number, number FROM numbers(1000);
SELECT count() FROM 04660_keeper_map_delete;

ALTER TABLE 04660_keeper_map_delete DELETE WHERE 1 SETTINGS max_block_size = 100;
SELECT count() FROM 04660_keeper_map_delete;

DROP TABLE 04660_keeper_map_delete SYNC;
