DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    col1 String,
    col2 String,
    INDEX test_table_col2_idx col2 TYPE set(0) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY col1
AS SELECT 'v1', 'v2';

SELECT * FROM tab
WHERE 1 == 1 AND col1 == col1 OR
       0 AND col2 == NULL;

DROP TABLE tab;

-- Test for issue #75485

SELECT 'Bulk filtering enabled';
set secondary_indices_enable_bulk_filtering = 1;

CREATE TABLE tab
(
    col Nullable(Boolean),
    INDEX col_idx col TYPE set(0)
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO tab VALUES
    (DEFAULT),
    (DEFAULT);

SELECT count() FROM tab WHERE col OR col IS NULL;
DROP TABLE tab;

CREATE TABLE tab
(
    col Nullable(Boolean),
    INDEX col_idx col TYPE set(0)
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO tab VALUES
    (DEFAULT),
    (DEFAULT),
    (TRUE);

SELECT count() FROM tab WHERE col OR col IS NULL;
DROP TABLE tab;

CREATE TABLE tab
(
    col Nullable(Boolean),
    INDEX col_idx col TYPE set(0)
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO tab VALUES
    (DEFAULT),
    (DEFAULT),
    (FALSE);

SELECT count() FROM tab WHERE col OR col IS NULL;
DROP TABLE tab;

SELECT 'Bulk filtering disabled';
set secondary_indices_enable_bulk_filtering = 0;

CREATE TABLE tab
(
    col Nullable(Boolean),
    INDEX col_idx col TYPE set(0)
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO tab VALUES
    (DEFAULT),
    (DEFAULT);

SELECT count() FROM tab WHERE col OR col IS NULL;
DROP TABLE tab;

CREATE TABLE tab
(
    col Nullable(Boolean),
    INDEX col_idx col TYPE set(0)
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO tab VALUES
    (DEFAULT),
    (DEFAULT),
    (TRUE);

SELECT count() FROM tab WHERE col OR col IS NULL;
DROP TABLE tab;

CREATE TABLE tab
(
    col Nullable(Boolean),
    INDEX col_idx col TYPE set(0)
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO tab VALUES
    (DEFAULT),
    (DEFAULT),
    (FALSE);

SELECT count() FROM tab WHERE col OR col IS NULL;
DROP TABLE tab;
