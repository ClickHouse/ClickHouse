SELECT 1 ? 1 : 0;
SELECT 0 ? not_existing_column : 1 FROM system.numbers LIMIT 1;
SELECT 1 ? (0 ? not_existing_column : 2) : 0 FROM system.numbers LIMIT 1;

/* scalar subquery optimization */
SELECT (SELECT toUInt8(number + 1) FROM system.numbers LIMIT 1) ? 1 : 2 FROM system.numbers LIMIT 1;

/* alias test */
SELECT (1 as a) ? (2 as b) : (3 as c) as d, a, b, c, d FORMAT TSKV;
SELECT (0 as a) ? (2 as b) : (3 as c) as d, a, b, c, d FORMAT TSKV;

SELECT (1 as a) ? (number + 2 as b) : (number + 3 as c) as d, a, b, c, d FROM system.numbers LIMIT 1 FORMAT TSKV;

/* intergration test */
SELECT (SELECT hasColumnInTable('system', 'numbers', 'not_existing')) ? not_existing : 42 as not_existing FROM system.numbers LIMIT 1 FORMAT TSKV;