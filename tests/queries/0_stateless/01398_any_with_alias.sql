SELECT any(number * number) AS n FROM numbers(100) FORMAT CSVWithNames;
SELECT any((number, number * 2)) as n FROM numbers(100) FORMAT CSVWithNames;
