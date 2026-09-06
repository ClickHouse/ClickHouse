-- `arrayJoin` changes the number of rows, while a TTL expression, a projection and a `Distributed`
-- sharding key are all applied positionally to the block. They are rejected at DDL time, like sorting
-- keys, partition keys and secondary indexes already are.

DROP TABLE IF EXISTS t_array_join_slots;
DROP TABLE IF EXISTS t_array_join_slots_dst;
DROP TABLE IF EXISTS t_array_join_slots_dist;

CREATE TABLE t_array_join_slots (k UInt32, d DateTime, arr Array(DateTime)) ENGINE = MergeTree ORDER BY k TTL arrayJoin(arr); -- { serverError BAD_TTL_EXPRESSION }

CREATE TABLE t_array_join_slots (k UInt32, arr Array(UInt32), PROJECTION p (SELECT count() GROUP BY arrayJoin(arr))) ENGINE = MergeTree ORDER BY k; -- { serverError INCORRECT_QUERY }
CREATE TABLE t_array_join_slots (k UInt32, arr Array(UInt32), PROJECTION p (SELECT count() GROUP BY unnest(arr))) ENGINE = MergeTree ORDER BY k; -- { serverError INCORRECT_QUERY }

CREATE TABLE t_array_join_slots_dst (k UInt32, arr Array(UInt32)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_array_join_slots_dist (k UInt32, arr Array(UInt32)) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_array_join_slots_dst', arrayJoin(arr)); -- { serverError ILLEGAL_COLUMN }

-- The same slots filled with an expression that keeps the row count are accepted.
CREATE TABLE t_array_join_slots (k UInt32, d DateTime, arr Array(DateTime), PROJECTION p (SELECT count() GROUP BY k)) ENGINE = MergeTree ORDER BY k TTL d + INTERVAL 1 DAY;
CREATE TABLE t_array_join_slots_dist (k UInt32, arr Array(UInt32)) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_array_join_slots_dst', k);
SELECT 'accepted';

ALTER TABLE t_array_join_slots MODIFY TTL arrayJoin(arr); -- { serverError BAD_TTL_EXPRESSION }
ALTER TABLE t_array_join_slots ADD PROJECTION p2 (SELECT count() GROUP BY arrayJoin(arr)); -- { serverError INCORRECT_QUERY }
ALTER TABLE t_array_join_slots MODIFY TTL d + INTERVAL 2 DAY;
SELECT 'altered';

DROP TABLE t_array_join_slots;
DROP TABLE t_array_join_slots_dist;
DROP TABLE t_array_join_slots_dst;
