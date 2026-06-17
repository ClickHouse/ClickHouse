-- Tags: distributed

-- { echo }

-- The limit/offset settings are a global cap applied once on the initiator AFTER the range step.
-- They must NOT leak to the shards, otherwise each shard would truncate to its first `limit` rows
-- before the boundary row is seen, and the range would open on the wrong (or empty) stream.
-- With 2 shards each holding 0..9, after ORDER BY the merged stream is 0,0,1,1,...,9,9.
-- LIMIT AFTER number >= 7 opens at the first 7 (producing 7,7,8,8,9,9), then limit = 2 caps to 7,7.
SELECT number FROM remote('127.0.0.{1,2}', numbers_mt(10)) ORDER BY number LIMIT AFTER number >= 7 SETTINGS limit = 2;
SELECT number FROM remote('127.0.0.{1,2}', numbers_mt(10)) ORDER BY number LIMIT AFTER number >= 7 SETTINGS limit = 2, enable_analyzer = 0;
-- With offset setting: skip the first matched row of the merged range, then cap.
SELECT number FROM remote('127.0.0.{1,2}', numbers_mt(10)) ORDER BY number LIMIT AFTER number >= 7 SETTINGS limit = 2, offset = 1;
SELECT number FROM remote('127.0.0.{1,2}', numbers_mt(10)) ORDER BY number LIMIT AFTER number >= 7 SETTINGS limit = 2, offset = 1, enable_analyzer = 0;
-- LIMIT UNTIL with settings limit.
SELECT number FROM remote('127.0.0.{1,2}', numbers_mt(10)) ORDER BY number LIMIT UNTIL number >= 3 SETTINGS limit = 3;
SELECT number FROM remote('127.0.0.{1,2}', numbers_mt(10)) ORDER BY number LIMIT UNTIL number >= 3 SETTINGS limit = 3, enable_analyzer = 0;
