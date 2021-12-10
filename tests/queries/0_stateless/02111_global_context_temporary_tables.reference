-- { echo }
SELECT * FROM remote('127.1', system.one, 1 IN id); -- { serverError UNKNOWN_TABLE }
SELECT * FROM remote('127.1', system.one, 1 IN dummy); -- { serverError UNKNOWN_TABLE }
SELECT * FROM remote('127.1', view(SELECT * FROM system.one), 1 IN id); -- { serverError UNKNOWN_TABLE }
SELECT * FROM remote('127.1', view(SELECT number AS id FROM numbers(2)), 1 IN id); -- { serverError UNKNOWN_TABLE }
