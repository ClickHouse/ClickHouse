SET send_logs_level = 'fatal';
select 1 = position('' in '');
select 1 = position('' in 'abc');
select 0 = position('abc' in '');
select 1 = position('abc' in 'abc');
select 2 = position('bc' in 'abc');
select 3 = position('c' in 'abc');

select 1 = position('' in '');
select 1 = position('' in 'абв');
select 0 = position('абв' in '');
select 1 = position('абв' in 'абв');
select 3 = position('бв' in 'абв');
select 5 = position('в' in 'абв');

select 6 = position('/' IN s) FROM (SELECT 'Hello/World' AS s);
