SET allow_deprecated_syntax_for_merge_tree = 1;
SET compile_sort_description = 1;
SET min_count_to_compile_sort_description = 0;

DROP TABLE IF EXISTS test1_00395;
CREATE TABLE test1_00395
(
    col1 UInt64,
    col2 Nullable(UInt64),
    col3 String,
    col4 Nullable(String),
    col5 Array(UInt64),
    col6 Array(Nullable(UInt64)),
    col7 Array(String),
    col8 Array(Nullable(String)),
    d Date
) Engine = MergeTree(d, (col1, d), 8192);

INSERT INTO test1_00395 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395 VALUES (1, NULL, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395 VALUES (1, 1, 'a', NULL, [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395 VALUES (1, 1, 'a', 'a', [1], [NULL], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], [NULL], '2000-01-01');

SELECT count(greatest(multiIf(1, 2, toNullable(NULL), 3, 4))) FROM test1_00395 WHERE toNullable(27) GROUP BY col1 ORDER BY multiIf(27, 1, multiIf(materialize(1), toLowCardinality(2), 3, 1, 4), NULL, 4) ASC NULLS LAST, col1 DESC;

SELECT '--';

SELECT count(greatest(multiIf(1, 2, toNullable(NULL), 3, 4))) FROM test1_00395 WHERE toNullable(27) GROUP BY col1 ORDER BY multiIf(27, 1, multiIf(materialize(1), toLowCardinality(2), 3, 1, 4), NULL, 4) ASC NULLS LAST, col1 DESC;

SELECT '--';

SELECT multiIf(1, 2, NULL, 3, 4), count(greatest(multiIf(1, 2, NULL, toUInt256(3), 4), multiIf(1, 2, NULL, 3, 4))) FROM test1_00395 GROUP BY col1 WITH CUBE WITH TOTALS ORDER BY multiIf(27, 1, multiIf(materialize(1), toLowCardinality(2), 3, 1, 4), NULL, 4) ASC NULLS LAST;

SELECT '--';

SELECT multiIf(1, 2, NULL, 3, 4), count(greatest(multiIf(1, 2, NULL, toUInt256(3), 4), multiIf(1, 2, NULL, 3, 4))) FROM test1_00395 GROUP BY col1 WITH CUBE WITH TOTALS ORDER BY multiIf(27, 1, multiIf(materialize(1), toLowCardinality(2), 3, 1, 4), NULL, 4) ASC NULLS LAST;

SELECT '--';

SELECT col1 FROM test1_00395 ORDER BY multiIf(27, 1, multiIf(materialize(1), toLowCardinality(2), 3, 1, 4), NULL, 4) ASC;

SELECT '--';

SELECT col1 FROM test1_00395 ORDER BY multiIf(27, 1, multiIf(materialize(1), toLowCardinality(2), 3, 1, 4), NULL, 4) ASC;

DROP TABLE test1_00395;
