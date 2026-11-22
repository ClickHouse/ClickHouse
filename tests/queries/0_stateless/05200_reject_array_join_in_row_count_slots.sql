-- `arrayJoin` changes the number of rows, while a projection and a `Distributed` sharding key are both
-- applied positionally to the block. They are rejected at DDL time, like sorting keys, partition keys
-- and secondary indexes already are. (TTL expressions are rejected by their own check in
-- `TTLDescription`, covered by `05073_ttl_array_join_rejected`.)

DROP TABLE IF EXISTS t_array_join_slots;
DROP TABLE IF EXISTS t_array_join_slots_dst;
DROP TABLE IF EXISTS t_array_join_slots_dist;

CREATE TABLE t_array_join_slots (k UInt32, arr Array(UInt32), PROJECTION p (SELECT count() GROUP BY arrayJoin(arr))) ENGINE = MergeTree ORDER BY k; -- { serverError INCORRECT_QUERY }

-- `unnest` is a case-insensitive alias of `arrayJoin`, and the rejection must not depend on whether the
-- name was canonicalized by the analyzer.
CREATE TABLE t_array_join_slots (k UInt32, arr Array(UInt32), PROJECTION p (SELECT count() GROUP BY unnest(arr))) ENGINE = MergeTree ORDER BY k; -- { serverError INCORRECT_QUERY }
CREATE TABLE t_array_join_slots (k UInt32, arr Array(UInt32), PROJECTION p (SELECT count() GROUP BY UNNEST(arr))) ENGINE = MergeTree ORDER BY k SETTINGS normalize_function_names = 0; -- { serverError INCORRECT_QUERY }

-- Nested, and behind a function that is not itself `arrayJoin`.
CREATE TABLE t_array_join_slots (k UInt32, arr Array(UInt32), PROJECTION p (SELECT sum(k) GROUP BY toUInt32(arrayJoin(arr) + 1))) ENGINE = MergeTree ORDER BY k; -- { serverError INCORRECT_QUERY }

CREATE TABLE t_array_join_slots_dst (k UInt32, arr Array(UInt32)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_array_join_slots_dist (k UInt32, arr Array(UInt32)) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_array_join_slots_dst', arrayJoin(arr)); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_array_join_slots_dist (k UInt32, arr Array(UInt32)) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_array_join_slots_dst', unnest(arr)); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_array_join_slots_dist (k UInt32, arr Array(UInt32)) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_array_join_slots_dst', unnest(arr)) SETTINGS normalize_function_names = 0; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_array_join_slots_dist (k UInt32, arr Array(UInt32)) ENGINE = Remote('127.0.0.1', currentDatabase(), 't_array_join_slots_dst', 'default', '', arrayJoin(arr)); -- { serverError ILLEGAL_COLUMN }

-- The same slots filled with an expression that keeps the row count are accepted.
CREATE TABLE t_array_join_slots (k UInt32, d DateTime, arr Array(DateTime), PROJECTION p (SELECT count() GROUP BY k)) ENGINE = MergeTree ORDER BY k TTL d + INTERVAL 1 DAY;
CREATE TABLE t_array_join_slots_dist (k UInt32, arr Array(UInt32)) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_array_join_slots_dst', k);
SELECT 'accepted';

-- A subquery has its own scope: `arrayJoin` inside it does not multiply the projection's rows.
CREATE TABLE t_array_join_slots_subquery (k UInt32, arr Array(UInt32), PROJECTION p (SELECT count() GROUP BY k IN (SELECT arrayJoin([1, 2])))) ENGINE = MergeTree ORDER BY k;
SELECT 'subquery accepted';

ALTER TABLE t_array_join_slots ADD PROJECTION p2 (SELECT count() GROUP BY arrayJoin(arr)); -- { serverError INCORRECT_QUERY }
ALTER TABLE t_array_join_slots ADD PROJECTION p2 (SELECT count() GROUP BY unnest(arr)); -- { serverError INCORRECT_QUERY }
ALTER TABLE t_array_join_slots MODIFY TTL d + INTERVAL 2 DAY;
-- An unrelated `ALTER` of a `Distributed` table does not revalidate the stored sharding key as fresh DDL.
ALTER TABLE t_array_join_slots_dist ADD COLUMN extra UInt8;
SELECT 'altered';

DROP TABLE t_array_join_slots;
DROP TABLE t_array_join_slots_subquery;
DROP TABLE t_array_join_slots_dist;
DROP TABLE t_array_join_slots_dst;
