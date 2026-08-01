-- { echo }
SELECT * FROM (SELECT (SELECT * FROM system.numbers LIMIT 1 OFFSET 1) AS n, toUInt64(10 / n)) FORMAT CSV;
SELECT (SELECT * FROM system.numbers LIMIT 1 OFFSET 1) AS n, toUInt64(10 / n) FORMAT CSV;
EXPLAIN SYNTAX SELECT (SELECT * FROM system.numbers LIMIT 1 OFFSET 1) AS n, toUInt64(10 / n);
SELECT * FROM (WITH (SELECT * FROM system.numbers LIMIT 1 OFFSET 1) AS n, toUInt64(10 / n) as q SELECT * FROM system.one WHERE q > 0);

SELECT * FROM (SELECT (SELECT '\d[a-z]') AS n, extractAll('5abc', assumeNotNull(n))) FORMAT CSV;
EXPLAIN SYNTAX SELECT (SELECT * FROM system.numbers LIMIT 1 OFFSET 1) AS n, toUInt64(10 / n);
