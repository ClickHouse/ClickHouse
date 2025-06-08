SET allow_experimental_dynamic_type=1;
SET allow_suspicious_types_in_order_by=1;

DROP TABLE IF EXISTS null_table;
CREATE TABLE null_table
(
    n1 UInt8,
    n2 Dynamic(max_types=3)
)
ENGINE = Null;

DROP TABLE IF EXISTS to_table;
CREATE TABLE to_table
(
    n1 UInt8,
    n2 Dynamic(max_types=4)
)
ENGINE = MergeTree ORDER BY n1;

DROP VIEW IF EXISTS dummy_rmv;
CREATE MATERIALIZED VIEW dummy_rmv TO to_table
AS SELECT * FROM null_table;

INSERT INTO null_table ( n1, n2 ) VALUES (1, '2024-01-01'), (2, toDateTime64('2024-01-01', 3, 'Asia/Istanbul')), (3, toFloat32(1)), (4, toFloat64(2));
SELECT *, dynamicType(n2) FROM to_table ORDER BY ALL;

select '';
INSERT INTO null_table ( n1, n2 ) VALUES (1, '2024-01-01'), (2, toDateTime64('2024-01-01', 3, 'Asia/Istanbul')), (3, toFloat32(1)), (4, toFloat64(2));
SELECT *, dynamicType(n2) FROM to_table ORDER BY ALL;

select '';
ALTER TABLE to_table MODIFY COLUMN n2 Dynamic(max_types=1);
SELECT *, dynamicType(n2) FROM to_table ORDER BY ALL;

select '';
ALTER TABLE to_table MODIFY COLUMN n2 Dynamic(max_types=10);
INSERT INTO null_table ( n1, n2 ) VALUES (1, '2024-01-01'), (2, toDateTime64('2024-01-01', 3, 'Asia/Istanbul')), (3, toFloat32(1)), (4, toFloat64(2));
SELECT *, dynamicType(n2) FROM to_table ORDER BY ALL;

DROP TABLE null_table;
DROP VIEW dummy_rmv;
DROP TABLE to_table;
