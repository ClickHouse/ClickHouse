-- `CLEAR COLUMN` recalculates the MATERIALIZED columns that read the cleared column, but that
-- recalculation rewrites values in place, without re-sorting rows or moving parts between
-- partitions. It must be refused when the sorting or partition key depends on such a column.

DROP TABLE IF EXISTS t_clear_key_dep;

CREATE TABLE t_clear_key_dep
(
    a UInt8,
    m UInt8 MATERIALIZED a + 1
) ENGINE = MergeTree
PARTITION BY m % 2
ORDER BY m;

INSERT INTO t_clear_key_dep SELECT number FROM numbers(10);

ALTER TABLE t_clear_key_dep CLEAR COLUMN a; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_clear_key_dep;

-- A sort-key expression source counts too.
CREATE TABLE t_clear_key_dep_expr
(
    a UInt8,
    m UInt8 MATERIALIZED a + 1
) ENGINE = MergeTree ORDER BY m + 1;

INSERT INTO t_clear_key_dep_expr SELECT number FROM numbers(10);

ALTER TABLE t_clear_key_dep_expr CLEAR COLUMN a; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_clear_key_dep_expr;

-- A transitive dependency (m2 reads m, the key reads m2) is refused as well.
CREATE TABLE t_clear_key_dep_transitive
(
    a UInt8,
    m UInt8 MATERIALIZED a + 1,
    m2 UInt8 MATERIALIZED m * 2
) ENGINE = MergeTree ORDER BY m2;

INSERT INTO t_clear_key_dep_transitive SELECT number FROM numbers(10);

ALTER TABLE t_clear_key_dep_transitive CLEAR COLUMN a; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_clear_key_dep_transitive;

-- Clearing is still allowed when the recalculated column is not read by any key.
CREATE TABLE t_clear_no_key_dep
(
    a UInt8,
    b UInt8,
    m UInt8 MATERIALIZED a + 1
) ENGINE = MergeTree ORDER BY b;

INSERT INTO t_clear_no_key_dep (a, b) SELECT number, number FROM numbers(3);

ALTER TABLE t_clear_no_key_dep CLEAR COLUMN a SETTINGS mutations_sync = 1;

SELECT a, b, m FROM t_clear_no_key_dep ORDER BY b;

DROP TABLE t_clear_no_key_dep;
