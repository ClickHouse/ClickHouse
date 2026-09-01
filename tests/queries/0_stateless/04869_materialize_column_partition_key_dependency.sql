-- Rewriting a column in place with `MATERIALIZE COLUMN` must be refused when the partition key
-- depends on it: the rows would stay in parts whose partition IDs and names were computed from
-- the old values. Same guard as for the sort key.

DROP TABLE IF EXISTS t_matcol_partkey;

CREATE TABLE t_matcol_partkey
(
    a UInt8,
    p UInt8 MATERIALIZED a + 1
)
ENGINE = MergeTree
PARTITION BY p % 2
ORDER BY a;

INSERT INTO t_matcol_partkey SELECT number FROM numbers(4);

ALTER TABLE t_matcol_partkey MODIFY COLUMN p UInt8 MATERIALIZED a + 2;

-- Refused before any mutation is queued.
ALTER TABLE t_matcol_partkey MATERIALIZE COLUMN p; -- { serverError CANNOT_UPDATE_COLUMN }

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_matcol_partkey';

-- Rows are still in the partitions computed from the original expression.
SELECT partition, sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 't_matcol_partkey' AND active GROUP BY partition ORDER BY partition;

DROP TABLE t_matcol_partkey;

-- An ordinary column with a mutable DEFAULT used by PARTITION BY takes the same guard.
CREATE TABLE t_defcol_partkey
(
    a UInt8,
    d UInt8 DEFAULT a + 1
)
ENGINE = MergeTree
PARTITION BY d % 2
ORDER BY a;

INSERT INTO t_defcol_partkey (a) SELECT number FROM numbers(4);

ALTER TABLE t_defcol_partkey MODIFY COLUMN d UInt8 DEFAULT a + 2;

ALTER TABLE t_defcol_partkey MATERIALIZE COLUMN d; -- { serverError CANNOT_UPDATE_COLUMN }

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_defcol_partkey';

DROP TABLE t_defcol_partkey;
