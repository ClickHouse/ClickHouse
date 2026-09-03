DROP TABLE IF EXISTS decimal_sum;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE decimal_sum
(
    date Date,
    sum32 Decimal32(4),
    sum64 Decimal64(8),
    sum128 Decimal128(10)
) Engine = SummingMergeTree(date, (date), 8192);

INSERT INTO decimal_sum VALUES ('2001-01-01', 1, 1, -1);
INSERT INTO decimal_sum VALUES ('2001-01-01', 1, -1, -1);

OPTIMIZE TABLE decimal_sum;
SELECT * FROM decimal_sum;

INSERT INTO decimal_sum VALUES ('2001-01-01', -2, 1, 2);

OPTIMIZE TABLE decimal_sum;
SELECT * FROM decimal_sum;

INSERT INTO decimal_sum VALUES ('2001-01-01', 0, -1, 0);

OPTIMIZE TABLE decimal_sum;
SELECT * FROM decimal_sum;

drop table decimal_sum;
