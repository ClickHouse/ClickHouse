SELECT any(number * number) AS n FROM numbers(100) FORMAT CSVWithNames;
EXPLAIN SYNTAX SELECT any(number * number) AS n FROM numbers(100);

SELECT any((number, number * 2)) as n FROM numbers(100) FORMAT CSVWithNames;
EXPLAIN SYNTAX SELECT any((number, number * 2)) as n FROM numbers(100);
