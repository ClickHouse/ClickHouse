-- Tests that a count-neutral change to the active-part set moves the MergeTree `modification_hash`.
-- A single-part `REPLACE PARTITION FROM` commits one added and one covered part in a single
-- transaction, so the net part-count delta is zero; the loop-free version folded into the hash must
-- advance on the membership change itself (parts added plus parts removed), not on the net delta,
-- otherwise such a transient replacement could go undetected by the pre/post consistency checks of
-- `query_cache_use_only_when_data_was_not_changed` and `REFRESH ... IF CHANGED` (the `A -> B -> A`
-- interleaving itself is a timing race that cannot be reproduced deterministically here; this test
-- pins the invariant that forecloses it). (Issue #108713, AI-review thread on PR #108721.)

DROP TABLE IF EXISTS t_replace;
DROP TABLE IF EXISTS t_source;
DROP TABLE IF EXISTS hashes_04825;

CREATE TABLE hashes_04825 (k String, v Nullable(UInt128)) ENGINE = Memory;

CREATE TABLE t_replace (p UInt64, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x;
CREATE TABLE t_source (p UInt64, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x;

-- State A: a single part in partition 1, holding the value 1.
INSERT INTO t_replace VALUES (1, 1);
INSERT INTO hashes_04825 SELECT 'A', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_replace';

-- State B: replace the partition with a single byte-identical part from the source table. The commit
-- adds one part and covers one part, so the net part count does not change.
INSERT INTO t_source VALUES (1, 1);
ALTER TABLE t_replace REPLACE PARTITION 1 FROM t_source;
INSERT INTO hashes_04825 SELECT 'B', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_replace';

-- Restore: another count-neutral replacement with the same content.
ALTER TABLE t_replace REPLACE PARTITION 1 FROM t_source;
INSERT INTO hashes_04825 SELECT 'A_again', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_replace';

SELECT '-- the hash is reported';
SELECT v IS NOT NULL FROM hashes_04825 WHERE k = 'A';
SELECT '-- a count-neutral replace changed the hash (A != B)';
SELECT (SELECT v FROM hashes_04825 WHERE k = 'A') != (SELECT v FROM hashes_04825 WHERE k = 'B');
SELECT '-- the restore changed the hash again (B != A_again)';
SELECT (SELECT v FROM hashes_04825 WHERE k = 'B') != (SELECT v FROM hashes_04825 WHERE k = 'A_again');
SELECT '-- loop-free: the round trip did NOT reproduce the earlier hash (A != A_again)';
SELECT (SELECT v FROM hashes_04825 WHERE k = 'A') != (SELECT v FROM hashes_04825 WHERE k = 'A_again');
SELECT '-- the data itself is unchanged across the round trip';
SELECT p, x FROM t_replace;

DROP TABLE t_replace;
DROP TABLE t_source;
DROP TABLE hashes_04825;
