-- { echoOn }
select toStartOfInterval(toDate32('2022-09-16'), INTERVAL 1 YEAR);
select toStartOfInterval(toDate32('2022-09-16'), INTERVAL 1 QUARTER);
select toStartOfInterval(toDate32('2022-09-16'), INTERVAL 1 MONTH);
select toStartOfInterval(toDate32('2022-09-16'), INTERVAL 1 WEEK);
select toStartOfInterval(toDate32('2022-09-16'), INTERVAL 1 DAY);
select toStartOfInterval(toDate32('2022-09-16'), INTERVAL 1 HOUR); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
select toStartOfInterval(toDate32('2022-09-16'), INTERVAL 1 MINUTE); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
select toStartOfInterval(toDate32('2022-09-16'), INTERVAL 1 SECOND); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
select toStartOfInterval(toDate32('2022-09-16'), INTERVAL 1 MILLISECOND); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
select toStartOfInterval(toDate32('2022-09-16'), INTERVAL 1 MICROSECOND); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
select toStartOfInterval(toDate32('2022-09-16'), INTERVAL 1 NANOSECOND); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }

select date_trunc('YEAR', toDate32('2022-09-16'));
select date_trunc('QUARTER', toDate32('2022-09-16'));
select date_trunc('MONTH', toDate32('2022-09-16'));
select date_trunc('WEEK', toDate32('2022-09-16'));
select date_trunc('DAY', toDate32('2022-09-16'));
select date_trunc('HOUR', toDate32('2022-09-16')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
select date_trunc('MINUTE', toDate32('2022-09-16')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
select date_trunc('SECOND', toDate32('2022-09-16')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
select date_trunc('MILLISECOND', toDate32('2022-09-16')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
select date_trunc('MICROSECOND', toDate32('2022-09-16')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
select date_trunc('NANOSECOND', toDate32('2022-09-16')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }


