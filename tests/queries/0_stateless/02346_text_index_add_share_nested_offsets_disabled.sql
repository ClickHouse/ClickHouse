-- The `ADD INDEX` permission preflight applies the commands as the table does, so with
-- `share_nested_offsets = 0` a column `n` is independent of `n.a` and is added before the index uses it.

DROP TABLE IF EXISTS t;

CREATE TABLE t (id UInt64, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id SETTINGS share_nested_offsets = 0;

ALTER TABLE t
    ADD COLUMN IF NOT EXISTS n UInt32,
    ADD INDEX idx(n) TYPE minmax GRANULARITY 1;

SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY name;
SELECT name, expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't';

DROP TABLE t;
