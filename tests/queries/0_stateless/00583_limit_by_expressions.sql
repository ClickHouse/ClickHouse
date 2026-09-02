SELECT 1 FROM system.one LIMIT 1 BY 1;
SELECT 1 FROM system.one LIMIT 1 BY 1 AS one;
SELECT 1 as one FROM system.one LIMIT 1 BY 1;
SELECT 1 as one FROM system.one LIMIT 1 BY one;
SELECT 1 as one FROM system.one LIMIT 1 BY rand();
SELECT number FROM numbers(10) LIMIT 2 BY number % 2;
SELECT number FROM numbers(10) LIMIT 2 BY intDiv(number, 5);
