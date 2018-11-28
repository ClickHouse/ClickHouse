SET send_logs_level = 'none';

select toDate('2018-01-01') < '2018-01-01';
select toDate('2018-01-01') == '2018-01-01';
select toDate('2018-01-01') != '2018-01-01';
select toDate('2018-01-01') < toDate('2018-01-01');
select toDate('2018-01-01') == toDate('2018-01-01');
select toDate('2018-01-01') != toDate('2018-01-01');

select toDate('2018-01-01') < 1;  -- { serverError 43 }
select toDate('2018-01-01') == 1; -- { serverError 43 }
select toDate('2018-01-01') != 1; -- { serverError 43 }

