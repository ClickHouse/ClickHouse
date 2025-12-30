SET send_logs_level = 'fatal';

select today() < 2018-11-14; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select toDate('2018-01-01') < '2018-11-14';

select toDate('2018-01-01') < '2018-01-01';
select toDate('2018-01-01') == '2018-01-01';
select toDate('2018-01-01') != '2018-01-01';
select toDate('2018-01-01') < toDate('2018-01-01');
select toDate('2018-01-01') == toDate('2018-01-01');
select toDate('2018-01-01') != toDate('2018-01-01');

select toDate('2018-01-01') < 1;  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select toDate('2018-01-01') == 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select toDate('2018-01-01') != 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }


