-- init
CREATE TABLE data
(
    `id` UInt32,
    `class` UInt32,
    `val` UInt32
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO data (*)
VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40);

-- tests
SELECT class, sum(val) FROM data GROUP BY class;
SELECT class, sum(val TOTALS) FROM data GROUP BY class;

-- cleanup
DROP TABLE data
