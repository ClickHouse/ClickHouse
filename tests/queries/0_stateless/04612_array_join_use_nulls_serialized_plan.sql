-- Round-trip of the `array_join_use_nulls` flag through query plan serialization:
-- force a real remote connection so the plan is serialized on the initiator and
-- deserialized on the remote side (query plan serialization version >= 4).

SET enable_analyzer = 1;
SET serialize_query_plan = 1;
SET prefer_localhost_replica = 0;
SET array_join_use_nulls = 1;

DROP TABLE IF EXISTS t_04612;
CREATE TABLE t_04612 (s String, arr Array(UInt8)) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_04612 VALUES ('Goodbye', []), ('Hello', [1, 2]), ('World', [3, 4, 5]);

SELECT s, arr, toTypeName(arr)
FROM remote('127.0.0.1', currentDatabase(), t_04612)
LEFT ARRAY JOIN arr
ORDER BY s, arr;

-- Regular ARRAY JOIN is unaffected by the setting and by the serialized flag.
SELECT s, arr, toTypeName(arr)
FROM remote('127.0.0.1', currentDatabase(), t_04612)
ARRAY JOIN arr
ORDER BY s, arr;

DROP TABLE t_04612;
