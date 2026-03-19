-- Test 1: Projection parts survive rename and reload
SET allow_experimental_physical_column_names = 1;

SELECT 'Test 1: projection parts survive rename and reload';
DROP TABLE IF EXISTS t_phys_proj;

CREATE TABLE t_phys_proj
(
    a UInt64,
    b String,
    c UInt64
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

ALTER TABLE t_phys_proj ADD PROJECTION p_sum (SELECT a, sum(c) GROUP BY a);

INSERT INTO t_phys_proj VALUES (1, 'one', 10);
INSERT INTO t_phys_proj VALUES (1, 'two', 20);
INSERT INTO t_phys_proj VALUES (2, 'three', 30);

SELECT a, sum(c) FROM t_phys_proj GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;

ALTER TABLE t_phys_proj RENAME COLUMN b TO d;

SELECT a, sum(c) FROM t_phys_proj GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;

OPTIMIZE TABLE t_phys_proj FINAL;

SELECT column, physical_name FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_phys_proj' AND active AND NOT startsWith(column, '_') ORDER BY column;

SELECT a, sum(c) FROM t_phys_proj GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;

DROP TABLE t_phys_proj;

-- Test 2: Flattened Nested with physical names is blocked with a clear error.
SELECT 'Test 2: flattened Nested guard';
DROP TABLE IF EXISTS t_phys_proj_flat;
CREATE TABLE t_phys_proj_flat (a UInt64, b String) ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;
ALTER TABLE t_phys_proj_flat ADD COLUMN n Nested(x UInt64, y String); -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_phys_proj_flat;

-- Test 3: Non-flattened Nested with physical names (flatten_nested = 0)
SELECT 'Test 3: non-flattened Nested with physical names';
SET flatten_nested = 0;
DROP TABLE IF EXISTS t_phys_nested_nf;

CREATE TABLE t_phys_nested_nf
(
    a UInt64,
    b String,
    n Nested(x UInt64, y String)
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

INSERT INTO t_phys_nested_nf VALUES (1, 'hello', [(10, 'a'), (20, 'b')]);
INSERT INTO t_phys_nested_nf VALUES (2, 'world', [(30, 'c')]);

ALTER TABLE t_phys_nested_nf RENAME COLUMN b TO d;

SELECT a, d, n.x, n.y FROM t_phys_nested_nf ORDER BY a;

OPTIMIZE TABLE t_phys_nested_nf FINAL;
SELECT a, d, n.x, n.y FROM t_phys_nested_nf ORDER BY a;

DROP TABLE t_phys_nested_nf;
SET flatten_nested = 1;
