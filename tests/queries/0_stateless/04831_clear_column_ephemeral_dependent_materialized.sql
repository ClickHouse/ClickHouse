-- A MATERIALIZED column that depends both on the cleared column and on an EPHEMERAL column cannot
-- be recalculated by the mutation, because EPHEMERAL values exist only during INSERT. `CLEAR COLUMN`
-- is still accepted, and such a column keeps the value stored at INSERT time.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_clear_ephemeral;

CREATE TABLE t_clear_ephemeral
(
    a UInt64,
    e UInt64 EPHEMERAL 7,
    m UInt64 MATERIALIZED a + e
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_clear_ephemeral (a, e) VALUES (1, 2);

ALTER TABLE t_clear_ephemeral CLEAR COLUMN a;
SELECT a, m FROM t_clear_ephemeral;

-- Clearing a column that no MATERIALIZED column reads leaves everything as is.
ALTER TABLE t_clear_ephemeral ADD COLUMN b UInt64;
ALTER TABLE t_clear_ephemeral CLEAR COLUMN b;
SELECT a, m FROM t_clear_ephemeral;

DROP TABLE t_clear_ephemeral;
