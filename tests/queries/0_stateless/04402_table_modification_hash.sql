-- Tests for the modification_hash column of system.tables (issue #108713).
-- modification_hash is a value that changes whenever the data behind the table changes.

DROP TABLE IF EXISTS t_mt;
DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_log;
DROP TABLE IF EXISTS hashes;

CREATE TABLE hashes (k String, v Nullable(UInt128)) ENGINE = Memory;

-- MergeTree: the hash must change on insert and on merge, and stay stable when nothing changes.
CREATE TABLE t_mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_mt VALUES (1);
INSERT INTO hashes SELECT 'mt_a', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_mt';
INSERT INTO hashes SELECT 'mt_a2', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_mt';
INSERT INTO t_mt VALUES (2);
INSERT INTO hashes SELECT 'mt_b', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_mt';
OPTIMIZE TABLE t_mt FINAL;
INSERT INTO hashes SELECT 'mt_c', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_mt';

-- Memory: changes on insert.
CREATE TABLE t_mem (x UInt64) ENGINE = Memory;
INSERT INTO t_mem VALUES (1);
INSERT INTO hashes SELECT 'mem_a', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_mem';
INSERT INTO t_mem VALUES (2);
INSERT INTO hashes SELECT 'mem_b', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_mem';

-- Log: changes on insert.
CREATE TABLE t_log (x UInt64) ENGINE = Log;
INSERT INTO t_log VALUES (1);
INSERT INTO hashes SELECT 'log_a', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_log';
INSERT INTO t_log VALUES (2);
INSERT INTO hashes SELECT 'log_b', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_log';

SELECT 'mt not null', (SELECT v FROM hashes WHERE k = 'mt_a') IS NOT NULL;
SELECT 'mt stable', (SELECT v FROM hashes WHERE k = 'mt_a') = (SELECT v FROM hashes WHERE k = 'mt_a2');
SELECT 'mt changed on insert', (SELECT v FROM hashes WHERE k = 'mt_a') != (SELECT v FROM hashes WHERE k = 'mt_b');
SELECT 'mt changed on merge', (SELECT v FROM hashes WHERE k = 'mt_b') != (SELECT v FROM hashes WHERE k = 'mt_c');
SELECT 'mem changed on insert', (SELECT v FROM hashes WHERE k = 'mem_a') != (SELECT v FROM hashes WHERE k = 'mem_b');
SELECT 'log changed on insert', (SELECT v FROM hashes WHERE k = 'log_a') != (SELECT v FROM hashes WHERE k = 'log_b');

-- System tables cannot tell whether their data changed: modification_hash is NULL.
SELECT 'system table null', modification_hash IS NULL FROM system.tables WHERE database = 'system' AND name = 'one';

DROP TABLE hashes;
DROP TABLE t_mt;
DROP TABLE t_mem;
DROP TABLE t_log;
