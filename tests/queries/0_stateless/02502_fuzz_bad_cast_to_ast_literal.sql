SET allow_deprecated_syntax_for_merge_tree=1;
DROP TABLE IF EXISTS test54378;
CREATE TABLE test54378 (`part_date` Date, `pk_date` Date, `date` Date) ENGINE = MergeTree(part_date, pk_date, 8192);
INSERT INTO test54378 values ('2018-04-19', '2018-04-19', '2018-04-19');
SELECT 232 FROM test54378 PREWHERE (part_date = (SELECT toDate('2018-04-19'))) IN (SELECT toDate('2018-04-19')) GROUP BY toDate(toDate(-2147483649, NULL), NULL), -inf;
DROP TABLE test54378;

