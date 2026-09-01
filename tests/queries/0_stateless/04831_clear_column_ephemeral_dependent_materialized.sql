-- CLEAR COLUMN must be rejected up front when a MATERIALIZED column depends both on
-- the cleared column and on an EPHEMERAL column: the recalculation cannot read
-- EPHEMERAL values from existing parts, so the mutation could never succeed.

DROP TABLE IF EXISTS t_clear_ephemeral;

CREATE TABLE t_clear_ephemeral
(
    a UInt64,
    e UInt64 EPHEMERAL 7,
    m UInt64 MATERIALIZED a + e
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_clear_ephemeral (a, e) VALUES (1, 2);

ALTER TABLE t_clear_ephemeral CLEAR COLUMN a SETTINGS mutations_sync = 1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- The ALTER was rejected before queueing a mutation.
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_clear_ephemeral';
SELECT a, m FROM t_clear_ephemeral;

-- Clearing a column that no EPHEMERAL-dependent MATERIALIZED column reads is still allowed.
ALTER TABLE t_clear_ephemeral ADD COLUMN b UInt64;
ALTER TABLE t_clear_ephemeral CLEAR COLUMN b SETTINGS mutations_sync = 1;
SELECT a, m FROM t_clear_ephemeral;

DROP TABLE t_clear_ephemeral;
