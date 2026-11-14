SET allow_experimental_dynamic_type = 1;
SET allow_suspicious_types_in_order_by = 1;
SET optimize_read_in_order = 1;

drop table if exists to_table;

CREATE TABLE to_table
(
    n1 UInt8,
    n2 Dynamic(max_types=2)
)
ENGINE = MergeTree ORDER BY n1;

INSERT INTO to_table ( n1, n2 ) VALUES (1, '2024-01-01'), (2, toDateTime64('2024-01-01', 3, 'Asia/Istanbul')), (3, toFloat32(1)), (4, toFloat64(2));
SELECT *, dynamicType(n2), isDynamicElementInSharedData(n2) FROM to_table ORDER BY ALL;

select '';
ALTER TABLE to_table MODIFY COLUMN n2 Dynamic(max_types=5);
INSERT INTO to_table ( n1, n2 ) VALUES (1, '2024-01-01'), (2, toDateTime64('2024-01-01', 3, 'Asia/Istanbul')), (3, toFloat32(1)), (4, toFloat64(2));
SELECT *, dynamicType(n2), isDynamicElementInSharedData(n2) FROM to_table ORDER BY ALL;

select '';
ALTER TABLE to_table MODIFY COLUMN n2 Dynamic(max_types=0);
INSERT INTO to_table ( n1, n2 ) VALUES (1, '2024-01-01'), (2, toDateTime64('2024-01-01', 3, 'Asia/Istanbul')), (3, toFloat32(1)), (4, toFloat64(2));
SELECT *, dynamicType(n2), isDynamicElementInSharedData(n2) FROM to_table ORDER BY ALL;

ALTER TABLE to_table MODIFY COLUMN n2 Dynamic(max_types=500); -- { serverError UNEXPECTED_AST_STRUCTURE }
