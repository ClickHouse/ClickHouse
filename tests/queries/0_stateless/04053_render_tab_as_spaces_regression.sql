-- https://github.com/ClickHouse/ClickHouse/pull/84605
SELECT position('a	a', '\t') > 0;
