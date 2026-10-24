-- An untyped NULL tuple element compares like a typed NULL, with or without Nullable around the tuple:
-- the other elements can decide the result, so a comparison and its negation never both filter a row out.

SELECT (1, 2) = toNullable((1, NULL)), (1, 2) != toNullable((1, NULL)), (1, 2) < toNullable((2, NULL)),
       (1, 2) > toNullable((0, NULL)), (1, 2) <= toNullable((0, NULL)), (1, 2) >= toNullable((2, NULL));
SELECT toNullable((1, NULL)) = (1, 2), toNullable((NULL, NULL)) = (1, 2), toNullable((1, NULL)) = toNullable((1, 2));
SELECT ((1, 2), 3) = (toNullable((1, NULL)), 3), ((1, 2), 3) = (toNullable((1, NULL)), 4), (toLowCardinality('a'), 1) = toNullable(('a', NULL));
SELECT (1, 2) > if(number = 0, NULL, (0, NULL)) FROM numbers(3);
SELECT materialize((1, 2)) = materialize(toNullable((number, NULL))) FROM numbers(3);
SELECT toTypeName((1, 2) > toNullable((0, NULL))), toTypeName((1, 2) = (3, NULL));

-- Decided by another element.
SELECT (1, 2) = toNullable((3, NULL)), (1, 2) != toNullable((3, NULL)), (1, 2) = (3, NULL), (1, 2) != (3, NULL);
SELECT (1, 2) < (2, NULL), (1, 2) > (0, NULL), (1, 2) <= (0, NULL), (1, 2) >= (2, NULL), (1, 2) < (1, NULL);
SELECT ([1], 2) = (NULL, 3), ([1], 2) != (NULL, 3), (map(1, 2), 1) != (NULL, 2), (NULL, 1) = (NULL, 2), (NULL, 1) = (NULL, 1);
SELECT materialize((1, 2)) = materialize(toNullable((3, NULL))), materialize((1, 2)) != materialize(toNullable((3, NULL)));

-- Same as a typed NULL element, and null-safe comparison is not affected.
SELECT (1, 2) = (3, CAST(NULL, 'Nullable(UInt8)')), (1, 2) != (3, CAST(NULL, 'Nullable(UInt8)'));
SELECT (1, NULL) <=> toNullable((1, NULL)), (1, 2) <=> toNullable((1, NULL)), (1, 2) <=> (3, NULL), (1, 2) > toNullable((0, NULL::Nullable(Int32)));

-- p, NOT p and p IS NULL together count every row; `_partition_value` filters parts by the inverted comparison.
DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl (dt DateTime, i Int32, j String) ENGINE = MergeTree PARTITION BY (toDate(dt), i % 2, length(j)) ORDER BY i;
INSERT INTO tbl VALUES ('2021-04-01 00:01:02', 1, '123'), ('2021-04-01 01:01:02', 1, '12'), ('2021-04-01 02:11:02', 2, '345'),
    ('2021-04-01 04:31:02', 2, '2'), ('2021-04-02 00:01:02', 1, '1234'), ('2021-04-02 00:01:02', 2, '123'),
    ('2021-04-02 00:01:02', 3, '12'), ('2021-04-02 00:01:02', 4, '1');
SELECT
    (SELECT count() FROM tbl WHERE _partition_value > toNullable((toDate('2021-04-01'), NULL, NULL))),
    (SELECT count() FROM tbl WHERE NOT (_partition_value > toNullable((toDate('2021-04-01'), NULL, NULL)))),
    (SELECT count() FROM tbl WHERE isNull(_partition_value > toNullable((toDate('2021-04-01'), NULL, NULL))));
-- Without the count optimizations the read itself filters the parts.
SELECT
    (SELECT count() FROM tbl WHERE _partition_value > toNullable((toDate('2021-04-01'), NULL, NULL))),
    (SELECT count() FROM tbl WHERE NOT (_partition_value > toNullable((toDate('2021-04-01'), NULL, NULL)))),
    (SELECT count() FROM tbl WHERE isNull(_partition_value > toNullable((toDate('2021-04-01'), NULL, NULL))))
SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0;
SELECT
    (SELECT count() FROM tbl WHERE _partition_value = toNullable((toDate('2021-04-01'), NULL, NULL))),
    (SELECT count() FROM tbl WHERE NOT (_partition_value = toNullable((toDate('2021-04-01'), NULL, NULL)))),
    (SELECT count() FROM tbl WHERE isNull(_partition_value = toNullable((toDate('2021-04-01'), NULL, NULL))))
SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0;
SELECT
    (SELECT count() FROM tbl WHERE _partition_value != (toDate('2021-04-02'), NULL, NULL)),
    (SELECT count() FROM tbl WHERE NOT (_partition_value != (toDate('2021-04-02'), NULL, NULL))),
    (SELECT count() FROM tbl WHERE isNull(_partition_value != (toDate('2021-04-02'), NULL, NULL)))
SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0;
SELECT countMerge(s) FROM (
    SELECT countState() AS s FROM tbl WHERE _partition_value > toNullable((toDate('2021-04-01'), NULL, NULL))
    UNION ALL SELECT countState() AS s FROM tbl WHERE NOT (_partition_value > toNullable((toDate('2021-04-01'), NULL, NULL)))
    UNION ALL SELECT countState() AS s FROM tbl WHERE isNull(_partition_value > toNullable((toDate('2021-04-01'), NULL, NULL))));
DROP TABLE tbl;
