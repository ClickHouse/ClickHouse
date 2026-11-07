-- A pending `ALTER TABLE ... CLEAR COLUMN c IN PARTITION p` used to be handed to the parts of every
-- partition, and a mutation that rewrote such a part baked the column's default into it: permanent data
-- loss outside the named partition. One `ALTER` with several commands is a single mutation entry, which
-- makes it deterministic - and on `ReplicatedMergeTree` the entry's partitions are the union over its
-- commands, so an unscoped sibling command exposed the scoped one to all partitions.

SELECT 'one entry with a scoped and an unscoped command, MergeTree';
DROP TABLE IF EXISTS t_clear_scoped_loss;
CREATE TABLE t_clear_scoped_loss (p UInt8, id UInt64, c UInt64 DEFAULT 777, d UInt64 DEFAULT 888)
ENGINE = MergeTree PARTITION BY p ORDER BY id SETTINGS min_bytes_for_wide_part = '10Mi';
INSERT INTO t_clear_scoped_loss VALUES (1, 1, 10, 100), (2, 2, 20, 200);

ALTER TABLE t_clear_scoped_loss CLEAR COLUMN c IN PARTITION '1', CLEAR COLUMN d SETTINGS mutations_sync = 2;
SELECT p, id, c, d FROM t_clear_scoped_loss ORDER BY id;

SELECT 'two scoped commands in one entry do not cross-apply';
DROP TABLE IF EXISTS t_clear_two_scopes;
CREATE TABLE t_clear_two_scopes (p UInt8, id UInt64, c UInt64 DEFAULT 777, d UInt64 DEFAULT 888)
ENGINE = MergeTree PARTITION BY p ORDER BY id SETTINGS min_bytes_for_wide_part = '10Mi';
INSERT INTO t_clear_two_scopes VALUES (1, 1, 10, 100), (2, 2, 20, 200);

ALTER TABLE t_clear_two_scopes CLEAR COLUMN c IN PARTITION '1', CLEAR COLUMN d IN PARTITION '2' SETTINGS mutations_sync = 2;
SELECT p, id, c, d FROM t_clear_two_scopes ORDER BY id;

SELECT 'a mutation racing a pending scoped CLEAR COLUMN keeps the other partitions';
DROP TABLE IF EXISTS t_clear_racing_loss;
CREATE TABLE t_clear_racing_loss (p UInt8, id UInt64, c UInt64 DEFAULT 777, d UInt64 DEFAULT 888)
ENGINE = MergeTree PARTITION BY p ORDER BY id SETTINGS min_bytes_for_wide_part = '10Mi';
INSERT INTO t_clear_racing_loss VALUES (1, 1, 10, 100), (2, 2, 20, 200);

-- Mutations of a plain MergeTree run in the background merge pool, so stopping merges keeps the first
-- one pending while the second is submitted; both run once merges are started again.
SYSTEM STOP MERGES t_clear_racing_loss;
ALTER TABLE t_clear_racing_loss CLEAR COLUMN c IN PARTITION '1' SETTINGS alter_sync = 0;
ALTER TABLE t_clear_racing_loss UPDATE d = d + 1 WHERE 1 SETTINGS alter_sync = 0;
SYSTEM START MERGES t_clear_racing_loss;
ALTER TABLE t_clear_racing_loss DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT p, id, c, d FROM t_clear_racing_loss ORDER BY id;

SELECT 'one entry with a scoped and an unscoped command, ReplicatedMergeTree';
DROP TABLE IF EXISTS t_clear_scoped_loss_r SYNC;
CREATE TABLE t_clear_scoped_loss_r (p UInt8, id UInt64, c UInt64 DEFAULT 777, d UInt64 DEFAULT 888)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_scoped_loss_r', '1')
PARTITION BY p ORDER BY id SETTINGS min_bytes_for_wide_part = '10Mi';
INSERT INTO t_clear_scoped_loss_r VALUES (1, 1, 10, 100), (2, 2, 20, 200);

ALTER TABLE t_clear_scoped_loss_r CLEAR COLUMN c IN PARTITION '1', CLEAR COLUMN d SETTINGS mutations_sync = 2;
SELECT p, id, c, d FROM t_clear_scoped_loss_r ORDER BY id;

-- Every `SELECT` above runs after its mutation materialized (`mutations_sync = 2`), so it reads the
-- stored data of the rewritten parts, not an on-the-fly conversion.

DROP TABLE t_clear_scoped_loss_r SYNC;
DROP TABLE t_clear_racing_loss;
DROP TABLE t_clear_two_scopes;
DROP TABLE t_clear_scoped_loss;
