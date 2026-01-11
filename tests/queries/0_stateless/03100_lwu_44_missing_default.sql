DROP TABLE IF EXISTS t_lwu_defaults;
DROP TABLE IF EXISTS t_mutation_defaults;

CREATE TABLE t_lwu_defaults
(
    x UInt32,
    y UInt32
)
ENGINE = MergeTree ORDER BY x
SETTINGS enable_block_offset_column = 1, enable_block_number_column = 1;

CREATE TABLE t_mutation_defaults
(
    x UInt32,
    y UInt32
)
ENGINE = MergeTree ORDER BY x;

INSERT INTO t_lwu_defaults (x, y) SELECT (number + 1) AS x, (x % 1000) AS y FROM numbers(9999);
INSERT INTO t_mutation_defaults (x, y) SELECT (number + 1) AS x, (x % 1000) AS y FROM numbers(9999);

ALTER TABLE t_lwu_defaults ADD COLUMN z UInt32 DEFAULT 0 AFTER y;
ALTER TABLE t_mutation_defaults ADD COLUMN z UInt32 DEFAULT 0 AFTER y;

UPDATE t_lwu_defaults SET z = y WHERE x > 0;
ALTER TABLE t_mutation_defaults UPDATE z = y WHERE x > 0 SETTINGS mutations_sync = 2;

SELECT intDiv(z, 100) AS a, COUNT() AS b FROM t_lwu_defaults GROUP BY a ORDER BY a LIMIT 10;
SELECT intDiv(z, 100) AS a, COUNT() AS b FROM t_mutation_defaults GROUP BY a ORDER BY a LIMIT 10;

DROP TABLE IF EXISTS t_lwu_defaults;
DROP TABLE IF EXISTS t_mutation_defaults;

CREATE TABLE t_lwu_defaults
(
    x UInt32,
    y UInt32
)
ENGINE = MergeTree ORDER BY x
SETTINGS enable_block_offset_column = 1, enable_block_number_column = 1;

CREATE TABLE t_mutation_defaults
(
    x UInt32,
    y UInt32
)
ENGINE = MergeTree ORDER BY x;

INSERT INTO t_lwu_defaults  (x, y) SELECT (number + 1) AS x, (x % 1000) AS y FROM numbers(9999);
INSERT INTO t_mutation_defaults (x, y) SELECT (number + 1) AS x, (x % 1000) AS y FROM numbers(9999);

ALTER TABLE t_lwu_defaults ADD COLUMN z UInt32 DEFAULT y + 1000 AFTER y;
ALTER TABLE t_mutation_defaults ADD COLUMN z UInt32 DEFAULT y + 1000 AFTER y;

UPDATE t_lwu_defaults SET y = y + 10000 WHERE x > 0;
ALTER TABLE t_mutation_defaults UPDATE y = y + 10000 WHERE x > 0 SETTINGS mutations_sync = 2;

SELECT intDiv(z, 100) AS a, COUNT() AS b FROM t_lwu_defaults GROUP BY a ORDER BY a LIMIT 10;
SELECT intDiv(z, 100) AS a, COUNT() AS b FROM t_mutation_defaults GROUP BY a ORDER BY a LIMIT 10;

DROP TABLE IF EXISTS t_lwu_defaults;
