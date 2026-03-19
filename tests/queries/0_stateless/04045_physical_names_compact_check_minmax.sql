SELECT 'Test 1: compact parts with physical names after RENAME';

DROP TABLE IF EXISTS t_phys_compact;

CREATE TABLE t_phys_compact
(
    a UInt64,
    b String,
    c UInt64
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    serialization_info_version = 'with_physical_names',
    allow_experimental_physical_column_names = 1,
    activate_physical_names_for_existing_tables = 1;

INSERT INTO t_phys_compact VALUES (1, 'one', 10);
INSERT INTO t_phys_compact VALUES (2, 'two', 20);

ALTER TABLE t_phys_compact RENAME COLUMN b TO d;

SELECT DISTINCT column, physical_name FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_phys_compact' AND active AND NOT startsWith(column, '_') ORDER BY column, physical_name;

SELECT a, d, c FROM t_phys_compact ORDER BY a;

INSERT INTO t_phys_compact VALUES (3, 'three', 30);
SELECT a, d, c FROM t_phys_compact ORDER BY a;

OPTIMIZE TABLE t_phys_compact FINAL;

SELECT DISTINCT column, physical_name FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_phys_compact' AND active AND NOT startsWith(column, '_') ORDER BY column, physical_name;

SELECT a, d, c FROM t_phys_compact ORDER BY a;

ALTER TABLE t_phys_compact ADD COLUMN e String DEFAULT 'hello';
INSERT INTO t_phys_compact VALUES (4, 'four', 40, 'world');
SELECT a, d, c, e FROM t_phys_compact ORDER BY a;

DROP TABLE t_phys_compact;

SELECT 'Test 2: minmax index with partition key column rename';

DROP TABLE IF EXISTS t_phys_minmax;

CREATE TABLE t_phys_minmax
(
    a UInt64,
    b String,
    dt Date
)
ENGINE = MergeTree
PARTITION BY dt
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    allow_experimental_physical_column_names = 1,
    activate_physical_names_for_existing_tables = 1;

INSERT INTO t_phys_minmax VALUES (1, 'one', '2024-01-01');
INSERT INTO t_phys_minmax VALUES (2, 'two', '2024-01-02');

ALTER TABLE t_phys_minmax RENAME COLUMN b TO d;

SELECT DISTINCT column, physical_name FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_phys_minmax' AND active AND NOT startsWith(column, '_') ORDER BY column, physical_name;

SELECT a, d, dt FROM t_phys_minmax ORDER BY a;

-- Verify partition pruning still works
SELECT a, d FROM t_phys_minmax WHERE dt = '2024-01-01' ORDER BY a;

DROP TABLE t_phys_minmax;
