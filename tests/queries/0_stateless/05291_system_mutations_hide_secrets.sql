-- Tags: no-fasttest
-- no-fasttest: the `s3` table function is not in the fast test build.

-- A mutation entry keeps the real text of its commands, because the mutation is executed from it (on every
-- replica, for replicated tables). `system.mutations.command` must still hide the credentials of the table
-- functions in the mutation subqueries, the way `system.query_log` does.

SET mutations_sync = 0;
SET lightweight_deletes_sync = 0;
SET allow_nondeterministic_mutations = 1;

DROP TABLE IF EXISTS t_05291;
DROP TABLE IF EXISTS t_05291_replicated;

CREATE TABLE t_05291 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05291_replicated (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_05291_replicated', 'r1') ORDER BY id;

INSERT INTO t_05291 SELECT number, number FROM numbers(10);
INSERT INTO t_05291_replicated SELECT number, number FROM numbers(10);

-- Keep the mutations pending, so that they stay in `system.mutations`.
SYSTEM STOP MERGES t_05291;
SYSTEM STOP MERGES t_05291_replicated;

ALTER TABLE t_05291 DELETE WHERE id IN
    (SELECT c1::UInt64 FROM s3('http://127.0.0.1:1/nope.csv', 'AKIAFAKEKEYID', 'SECRET05291DELETE', 'CSV', 'c1 String'));
ALTER TABLE t_05291 UPDATE v = 0 WHERE id IN
    (SELECT c1::UInt64 FROM s3('http://127.0.0.1:1/nope.csv', 'AKIAFAKEKEYID', 'SECRET05291UPDATE', 'CSV', 'c1 String'));
DELETE FROM t_05291 WHERE id IN
    (SELECT c1::UInt64 FROM s3('http://127.0.0.1:1/nope.csv', 'AKIAFAKEKEYID', 'SECRET05291LIGHTWEIGHT', 'CSV', 'c1 String'));
ALTER TABLE t_05291_replicated DELETE WHERE id IN
    (SELECT c1::UInt64 FROM s3('http://127.0.0.1:1/nope.csv', 'AKIAFAKEKEYID', 'SECRET05291REPLICATED', 'CSV', 'c1 String'));

SELECT table, command FROM system.mutations WHERE database = currentDatabase() ORDER BY table, mutation_id;

-- A secret cannot be probed with a filter on the column either.
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND position(command, 'SECRET05291') > 0;

-- `KILL MUTATION` parses the command that it reads from `system.mutations` to check the access rights.
KILL MUTATION WHERE database = currentDatabase() AND table = 't_05291' FORMAT Null;

DROP TABLE t_05291;
DROP TABLE t_05291_replicated;
