-- Late inserts are resolved by the per-column versions persisted in merged parts.

DROP TABLE IF EXISTS t_vcmt_late;

CREATE TABLE t_vcmt_late
(
    key UInt64,
    version UInt64,
    a Nullable(UInt64),
    b Nullable(String)
)
ENGINE = VersionedCoalescingMergeTree(version)
ORDER BY key;

INSERT INTO t_vcmt_late VALUES (1, 5, 50, NULL);
INSERT INTO t_vcmt_late VALUES (1, 1, 10, 'old');

OPTIMIZE TABLE t_vcmt_late FINAL;
SELECT 'after the first merge';
-- The merged row remembers that the value of b comes from version 1.
SELECT *, _column_versions FROM t_vcmt_late;

-- A late insert: its version is lower than the merged row, but higher than the version of b.
INSERT INTO t_vcmt_late VALUES (1, 3, 100, 'new');

SELECT 'FINAL resolves the late row';
SELECT * FROM t_vcmt_late FINAL;

OPTIMIZE TABLE t_vcmt_late FINAL;
SELECT 'after the second merge';
SELECT *, _column_versions FROM t_vcmt_late;

DROP TABLE t_vcmt_late;
