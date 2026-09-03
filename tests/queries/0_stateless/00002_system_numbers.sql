SET send_logs_level = 'fatal';

SELECT * FROM system.numbers LIMIT 3;
SELECT sys_num.number FROM system.numbers AS sys_num WHERE number > 2 LIMIT 2;
SELECT number FROM system.numbers WHERE number >= 5 LIMIT 2;
SELECT * FROM system.numbers WHERE number == 7 LIMIT 1;
SELECT number AS n FROM system.numbers WHERE number IN(8, 9) LIMIT 2;
select number from system.numbers limit 0;
select x from system.numbers limit 1; -- { serverError UNKNOWN_IDENTIFIER }
SELECT x, number FROM system.numbers LIMIT 1; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * FROM system.number LIMIT 1; -- { serverError UNKNOWN_TABLE }
SELECT * FROM system LIMIT 1; -- { serverError UNKNOWN_TABLE }
SELECT * FROM numbers LIMIT 1; -- { serverError UNKNOWN_TABLE }
SELECT sys.number FROM system.numbers AS sys_num LIMIT 1; -- { serverError UNKNOWN_IDENTIFIER }
