-- Tests that a MergeTree modification_hash is loop-free: an A -> B -> A transition of the active-part
-- set (INSERT then DROP PART/PARTITION returns the set to an identical value) must NOT return to the
-- earlier hash. The query-cache (`query_cache_use_only_when_data_was_not_changed`) and
-- `REFRESH ... IF CHANGED` consumers validate consistency by comparing the referenced-tables hash
-- before and after the read. If the hash could repeat across a round trip, a concurrent A -> B -> A
-- would make that pre/post check pass while the read actually saw the transient state B, letting a
-- result built from B be stored under state A's key. The round trip itself is a timing race that cannot
-- be reproduced deterministically here; this test proves the invariant that forecloses it -- the hash
-- after the round trip differs from the hash before it, so the pre/post check detects the change.
-- (issue #108713, AI-review thread on PR #108721.)

DROP TABLE IF EXISTS t_aba;
DROP TABLE IF EXISTS hashes;

CREATE TABLE hashes (k String, v Nullable(UInt128)) ENGINE = Memory;

-- One part per partition, so DROP PARTITION returns the active-part set to exactly its earlier value
-- without any merge or part renaming.
CREATE TABLE t_aba (p UInt64, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x;

-- State A: a single partition.
INSERT INTO t_aba VALUES (1, 1);
INSERT INTO hashes SELECT 'A', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_aba';

-- State B: a second partition.
INSERT INTO t_aba VALUES (2, 2);
INSERT INTO hashes SELECT 'B', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_aba';

-- Back to state A's data: drop the second partition. The active-part set is byte-for-byte the initial
-- one, so without the loop-free version this would reproduce hash A.
ALTER TABLE t_aba DROP PARTITION 2;
INSERT INTO hashes SELECT 'A_again', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_aba';

SELECT '-- hash changed on insert (A != B)';
SELECT (SELECT v FROM hashes WHERE k = 'A') != (SELECT v FROM hashes WHERE k = 'B');
SELECT '-- hash changed on drop (B != A_again)';
SELECT (SELECT v FROM hashes WHERE k = 'B') != (SELECT v FROM hashes WHERE k = 'A_again');
SELECT '-- loop-free: the round trip did NOT reproduce the earlier hash (A != A_again)';
SELECT (SELECT v FROM hashes WHERE k = 'A') != (SELECT v FROM hashes WHERE k = 'A_again');

-- The same must hold for DETACH PART / ATTACH PART, which can also restore an identical active-part set.
DROP TABLE IF EXISTS t_aba2;
CREATE TABLE t_aba2 (p UInt64, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x;
INSERT INTO t_aba2 VALUES (1, 1);
INSERT INTO hashes SELECT 'D_A', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_aba2';
INSERT INTO t_aba2 VALUES (2, 2);
ALTER TABLE t_aba2 DETACH PARTITION 2;
INSERT INTO hashes SELECT 'D_A_again', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_aba2';

SELECT '-- loop-free across DETACH PARTITION too (D_A != D_A_again)';
SELECT (SELECT v FROM hashes WHERE k = 'D_A') != (SELECT v FROM hashes WHERE k = 'D_A_again');

-- The hash must also be loop-free for read-affecting metadata-only changes that touch no parts. Changing
-- a column's ALIAS expression A -> B -> A changes query results (SELECT y) without rewriting any data, so
-- the folded column strings return to their earlier value and the active-part counter does not move; only
-- the metadata version keeps the round trip from reproducing the earlier hash.
DROP TABLE IF EXISTS t_meta;
CREATE TABLE t_meta (x UInt64, y UInt64 ALIAS x + 1) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_meta VALUES (10);
INSERT INTO hashes SELECT 'M_A', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_meta';
ALTER TABLE t_meta MODIFY COLUMN y UInt64 ALIAS x + 2;
INSERT INTO hashes SELECT 'M_B', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_meta';
ALTER TABLE t_meta MODIFY COLUMN y UInt64 ALIAS x + 1;
INSERT INTO hashes SELECT 'M_A_again', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_meta';

SELECT '-- hash changed on metadata change (M_A != M_B)';
SELECT (SELECT v FROM hashes WHERE k = 'M_A') != (SELECT v FROM hashes WHERE k = 'M_B');
SELECT '-- hash changed back on metadata revert (M_B != M_A_again)';
SELECT (SELECT v FROM hashes WHERE k = 'M_B') != (SELECT v FROM hashes WHERE k = 'M_A_again');
SELECT '-- loop-free across a metadata round trip (M_A != M_A_again)';
SELECT (SELECT v FROM hashes WHERE k = 'M_A') != (SELECT v FROM hashes WHERE k = 'M_A_again');

DROP TABLE t_aba;
DROP TABLE t_aba2;
DROP TABLE t_meta;
DROP TABLE hashes;
