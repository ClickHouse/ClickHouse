SELECT 1 ? 1 : 0;
SELECT 0 ? not_existing_column : 1 FROM system.numbers LIMIT 1;
SELECT if(1, if(0, not_existing_column, 2), 0) FROM system.numbers LIMIT 1;

SELECT (SELECT hasColumnInTable('system', 'numbers', 'not_existing')) ? not_existing : 42 FROM system.numbers LIMIT 1;
