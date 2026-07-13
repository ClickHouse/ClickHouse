-- Basic countIf with asterisk
SELECT countIf(*, number < 5) FROM numbers(10);

-- countIf with asterisk and multiple columns in subquery
SELECT countIf(*, number < 20) FROM (SELECT number, 1, 2 FROM numbers(100));

-- count with filter syntax
SELECT count(*) FILTER (WHERE number < 20) FROM (SELECT number, 1, 2 FROM numbers(100));
