DROP TABLE IF EXISTS t_lwu_missing_default;
DROP TABLE IF EXISTS t_mutation_missing_default;

-- The updated column `a` and the `DEFAULT` columns that depend on it are added after the part is
-- written, so none of them is stored in the part and all of them are materialized on read.
-- A lightweight `UPDATE` must give the same result as the equivalent `ALTER TABLE ... UPDATE`.

CREATE TABLE t_lwu_missing_default (x UInt32) ENGINE = MergeTree ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE t_mutation_missing_default (x UInt32) ENGINE = MergeTree ORDER BY x;

INSERT INTO t_lwu_missing_default SELECT number FROM numbers(6);
INSERT INTO t_mutation_missing_default SELECT number FROM numbers(6);

ALTER TABLE t_lwu_missing_default ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_missing_default ADD COLUMN z UInt32 DEFAULT a + 1000;
ALTER TABLE t_lwu_missing_default ADD COLUMN w UInt32 DEFAULT z * 2;

ALTER TABLE t_mutation_missing_default ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_mutation_missing_default ADD COLUMN z UInt32 DEFAULT a + 1000;
ALTER TABLE t_mutation_missing_default ADD COLUMN w UInt32 DEFAULT z * 2;

UPDATE t_lwu_missing_default SET a = x + 5 WHERE x % 2 = 0;
ALTER TABLE t_mutation_missing_default UPDATE a = x + 5 WHERE x % 2 = 0 SETTINGS mutations_sync = 2;

SELECT 'lightweight';
SELECT x, a, z, w FROM t_lwu_missing_default ORDER BY x;

SELECT 'heavy';
SELECT x, a, z, w FROM t_mutation_missing_default ORDER BY x;

-- The updated column is not requested by the query: it is read only to materialize the dependents.

SELECT 'lightweight, dependents only';
SELECT z, w FROM t_lwu_missing_default ORDER BY z, w;

SELECT 'heavy, dependents only';
SELECT z, w FROM t_mutation_missing_default ORDER BY z, w;

DROP TABLE t_lwu_missing_default;
DROP TABLE t_mutation_missing_default;

DROP TABLE IF EXISTS t_lwu_stored_default;

-- The updated column is stored in the part: the dependent `DEFAULT` column must still see the patch.

CREATE TABLE t_lwu_stored_default (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_stored_default SELECT number, number FROM numbers(6);
ALTER TABLE t_lwu_stored_default ADD COLUMN z UInt32 DEFAULT y + 1000;

UPDATE t_lwu_stored_default SET y = y + 100 WHERE x % 2 = 0;

SELECT 'lightweight, updated column stored';
SELECT x, y, z FROM t_lwu_stored_default ORDER BY x;

DROP TABLE t_lwu_stored_default;
