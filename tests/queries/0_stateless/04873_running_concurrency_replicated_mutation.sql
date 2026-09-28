-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: non-deterministic mutations are allowed with shared merge tree

-- A mutation on a replicated table replays on every replica, so its expression must be
-- deterministic. `runningConcurrency` reads the rows processed before it, so it is refused
-- there, with `allow_nondeterministic_mutations` as the escape hatch. A plain `MergeTree`
-- table enforces nothing.

DROP TABLE IF EXISTS repl_04873 SYNC;
DROP TABLE IF EXISTS plain_04873 SYNC;

CREATE TABLE repl_04873 (x UInt32, v UInt32, s DateTime, e DateTime)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04873/t', 'r1') ORDER BY x;
INSERT INTO repl_04873 VALUES (1, 0, '2020-01-01 00:00:00', '2020-01-01 00:00:10');

ALTER TABLE repl_04873 UPDATE v = runningConcurrency(s, e) WHERE 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE repl_04873 UPDATE v = runningConcurrency(s, e) WHERE 1
    SETTINGS allow_nondeterministic_mutations = 1;

-- Sibling control: a running function that was already non-deterministic is refused the same
-- way, so the row above is a statement about the declaration, not about the mutation path.
ALTER TABLE repl_04873 UPDATE v = rowNumberInAllBlocks() WHERE 1; -- { serverError BAD_ARGUMENTS }

-- Negative control: a deterministic expression needs no hatch.
ALTER TABLE repl_04873 UPDATE v = x + 1 WHERE 1;

-- Contrast: no such requirement on a non-replicated table.
CREATE TABLE plain_04873 (x UInt32, v UInt32, s DateTime, e DateTime) ENGINE = MergeTree ORDER BY x;
INSERT INTO plain_04873 VALUES (1, 0, '2020-01-01 00:00:00', '2020-01-01 00:00:10');
ALTER TABLE plain_04873 UPDATE v = runningConcurrency(s, e) WHERE 1;

SELECT 'ok';

DROP TABLE repl_04873 SYNC;
DROP TABLE plain_04873 SYNC;
