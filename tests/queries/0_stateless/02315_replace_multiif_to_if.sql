EXPLAIN SYNTAX SELECT multiIf(number = 0, NULL, toNullable(number)) FROM numbers(10000);
EXPLAIN SYNTAX SELECT CASE WHEN number = 0 THEN NULL ELSE toNullable(number) END FROM numbers(10000);
