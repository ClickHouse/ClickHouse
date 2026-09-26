-- A projection part is read without the parent part's `AlterConversions`, so while the mutation of
-- `DROP COLUMN c` is pending, the projection part still carries the old `c`. After the column and the
-- projection are re-added under the same names, a query served from the projection returned the old
-- values of `c` instead of the default of the new column. Projections are not used while a
-- `RENAME COLUMN` / `DROP COLUMN` mutation is pending.

DROP TABLE IF EXISTS t_readd_projection;
CREATE TABLE t_readd_projection (id UInt64, c UInt64, PROJECTION p (SELECT id, c ORDER BY c))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 128;
INSERT INTO t_readd_projection SELECT number, number % 100 FROM numbers(100000);

SYSTEM STOP MERGES t_readd_projection;
ALTER TABLE t_readd_projection DROP PROJECTION p SETTINGS alter_sync = 0;
ALTER TABLE t_readd_projection DROP COLUMN c SETTINGS alter_sync = 0;
ALTER TABLE t_readd_projection ADD COLUMN c UInt32 SETTINGS alter_sync = 0;
ALTER TABLE t_readd_projection ADD PROJECTION p (SELECT id, c ORDER BY c) SETTINGS alter_sync = 0;

SELECT 'normal projection';
SELECT count() FROM t_readd_projection WHERE c = 5;
SELECT count() FROM t_readd_projection WHERE c = 5 SETTINGS optimize_use_projections = 0;

DROP TABLE IF EXISTS t_readd_agg_projection;
CREATE TABLE t_readd_agg_projection (id UInt64, c UInt64, PROJECTION p (SELECT sum(c)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_readd_agg_projection SELECT number, number + 100 FROM numbers(1000);

SYSTEM STOP MERGES t_readd_agg_projection;
ALTER TABLE t_readd_agg_projection DROP PROJECTION p SETTINGS alter_sync = 0;
ALTER TABLE t_readd_agg_projection DROP COLUMN c SETTINGS alter_sync = 0;
ALTER TABLE t_readd_agg_projection ADD COLUMN c UInt32 SETTINGS alter_sync = 0;
ALTER TABLE t_readd_agg_projection ADD PROJECTION p (SELECT sum(c)) SETTINGS alter_sync = 0;

SELECT 'aggregate projection';
SELECT sum(c) FROM t_readd_agg_projection;
SELECT sum(c) FROM t_readd_agg_projection SETTINGS optimize_use_projections = 0;

SELECT 'the same answer once the mutations have rewritten the parts';
SYSTEM START MERGES t_readd_projection;
SYSTEM START MERGES t_readd_agg_projection;
ALTER TABLE t_readd_projection DELETE WHERE 0 SETTINGS mutations_sync = 2;
ALTER TABLE t_readd_agg_projection DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT count() FROM t_readd_projection WHERE c = 5;
SELECT sum(c) FROM t_readd_agg_projection;

DROP TABLE t_readd_agg_projection;
DROP TABLE t_readd_projection;
