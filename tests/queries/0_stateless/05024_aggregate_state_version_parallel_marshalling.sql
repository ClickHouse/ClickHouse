-- The parallel block marshalling path serializes columns into blobs before `NativeWriter` announces
-- the types, so it has to derive the state version of versioned aggregate functions from the
-- negotiated revision the same way `NativeWriter` does. It used to take the version from the local
-- type instead, so the payload was written at version 0 while the announced type said version 1,
-- and the receiver lost sync inside the blob.

DROP TABLE IF EXISTS marshalling_source;
CREATE TABLE marshalling_source (s String, n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO marshalling_source SELECT toString(number), number FROM numbers(200000);

-- Partial aggregation states of `uniq` cross the wire inside marshalled blocks.
SELECT sum(u) FROM
(
    SELECT uniq(s) AS u
    FROM remote('127.0.0.{1,2}', currentDatabase(), marshalling_source)
    GROUP BY n % 2
)
SETTINGS enable_parallel_blocks_marshalling = 1, prefer_localhost_replica = 0;

-- The same for `quantileDeterministic` (state version 1 appends a skip degree to the state).
SELECT medianDeterministic(n, n)
FROM remote('127.0.0.{1,2}', currentDatabase(), marshalling_source)
SETTINGS enable_parallel_blocks_marshalling = 1, prefer_localhost_replica = 0;

-- A raw state column read through a distributed query is marshalled as well.
DROP TABLE IF EXISTS marshalling_states;
CREATE TABLE marshalling_states (state AggregateFunction(uniq, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO marshalling_states SELECT uniqState(number) FROM numbers(1000000);

SELECT uniqMerge(state)
FROM (SELECT state FROM remote('127.0.0.2', currentDatabase(), marshalling_states))
SETTINGS enable_parallel_blocks_marshalling = 1, prefer_localhost_replica = 0;

DROP TABLE marshalling_source;
DROP TABLE marshalling_states;
