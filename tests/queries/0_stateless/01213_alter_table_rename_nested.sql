DROP TABLE IF EXISTS table_for_rename_nested;
CREATE TABLE table_for_rename_nested
(
    date Date,
    key UInt64,
    n Nested(x UInt32, y String),
    value1 String
)
ENGINE = MergeTree()
PARTITION BY date
ORDER BY key;

INSERT INTO table_for_rename_nested (date, key, n.x, n.y, value1) SELECT toDate('2019-10-01'), number, [number + 1, number + 2, number + 3], ['a', 'b', 'c'], toString(number) FROM numbers(10);

SELECT n.x FROM table_for_rename_nested WHERE key = 7;
SELECT n.y FROM table_for_rename_nested WHERE key = 7;

SHOW CREATE TABLE table_for_rename_nested;

ALTER TABLE table_for_rename_nested RENAME COLUMN n.x TO n.renamed_x;
ALTER TABLE table_for_rename_nested RENAME COLUMN n.y TO n.renamed_y;

SHOW CREATE TABLE table_for_rename_nested;

SELECT key, n.renamed_x FROM table_for_rename_nested WHERE key = 7;
SELECT key, n.renamed_y FROM table_for_rename_nested WHERE key = 7;

ALTER TABLE table_for_rename_nested RENAME COLUMN n.renamed_x TO not_nested_x; --{serverError 36}

ALTER TABLE table_for_rename_nested RENAME COLUMN n.renamed_x TO q.renamed_x; --{serverError 36}

ALTER TABLE table_for_rename_nested RENAME COLUMN value1 TO q.renamed_x; --{serverError 36}

-- Currently not implemented
ALTER TABLE table_for_rename_nested RENAME COLUMN n TO renamed_n; --{serverError 48}

DROP TABLE IF EXISTS table_for_rename_nested;

