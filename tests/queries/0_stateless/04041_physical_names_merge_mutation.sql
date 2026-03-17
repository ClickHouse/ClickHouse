DROP TABLE IF EXISTS t_physical_merge;

CREATE TABLE t_physical_merge
(
    a UInt64,
    b String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names';

INSERT INTO t_physical_merge VALUES (1, 'one');
INSERT INTO t_physical_merge VALUES (2, 'two');

ALTER TABLE t_physical_merge ADD COLUMN c Nullable(UInt64);

INSERT INTO t_physical_merge (a, b, c) VALUES (3, 'three', 30);
INSERT INTO t_physical_merge (a, b, c) VALUES (4, 'four', 40);

OPTIMIZE TABLE t_physical_merge FINAL;
SELECT * FROM t_physical_merge ORDER BY a;

SELECT count()
FROM system.parts
WHERE database = currentDatabase() AND table = 't_physical_merge' AND active;

DROP TABLE t_physical_merge;

-- Test compact parts with physical names
DROP TABLE IF EXISTS t_physical_compact;

CREATE TABLE t_physical_compact
(
    a UInt64,
    b String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    serialization_info_version = 'with_physical_names';

INSERT INTO t_physical_compact VALUES (1, 'one');

ALTER TABLE t_physical_compact ADD COLUMN c UInt64 DEFAULT 0;

INSERT INTO t_physical_compact (a, b, c) VALUES (2, 'two', 22);
SELECT * FROM t_physical_compact ORDER BY a;

INSERT INTO t_physical_compact (a, b, c) VALUES (3, 'three', 33);
OPTIMIZE TABLE t_physical_compact FINAL;
SELECT * FROM t_physical_compact ORDER BY a;

DROP TABLE t_physical_compact;

-- Test mutation (MODIFY COLUMN) with physical names
DROP TABLE IF EXISTS t_physical_mutation;

CREATE TABLE t_physical_mutation
(
    a UInt64,
    b String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names';

ALTER TABLE t_physical_mutation ADD COLUMN c UInt32 DEFAULT 0;
INSERT INTO t_physical_mutation (a, b, c) VALUES (1, 'one', 10);
INSERT INTO t_physical_mutation (a, b, c) VALUES (2, 'two', 20);

ALTER TABLE t_physical_mutation MODIFY COLUMN c UInt64;
SELECT c, toTypeName(c) FROM t_physical_mutation ORDER BY a;

OPTIMIZE TABLE t_physical_mutation FINAL;
SELECT c, toTypeName(c) FROM t_physical_mutation ORDER BY a;

DROP TABLE t_physical_mutation;
