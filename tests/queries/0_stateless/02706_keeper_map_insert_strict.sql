-- Tags: no-ordinary-database, no-fasttest

DROP TABLE IF EXISTS 02706_keeper_map_insert_strict SYNC;

CREATE TABLE 02706_keeper_map_insert_strict (key UInt64, value Float64) Engine=KeeperMap('/' || currentDatabase() || '/test_02706_keeper_map_insert_strict') PRIMARY KEY(key);

INSERT INTO 02706_keeper_map_insert_strict VALUES (1, 1.1), (2, 2.2);
SELECT * FROM 02706_keeper_map_insert_strict WHERE key = 1;

SET keeper_map_strict_mode = false;

INSERT INTO 02706_keeper_map_insert_strict VALUES (1, 2.1);
SELECT * FROM 02706_keeper_map_insert_strict WHERE key = 1;

SET keeper_map_strict_mode = true;

INSERT INTO 02706_keeper_map_insert_strict VALUES (1, 2.1); -- { serverError KEEPER_EXCEPTION }
SELECT * FROM 02706_keeper_map_insert_strict WHERE key = 1;

DROP TABLE 02706_keeper_map_insert_strict;
