-- dateTime64Diff
select '-- dateTime64Diff --';
select dateTime64Diff(toDateTime64('2022-11-23 09:26:37.123456789', 9), toDateTime64('2022-01-01', 0));
select dateTime64Diff(toDateTime64('2022-01-01', 0), toDateTime64('2022-11-23 09:26:37.123456789', 9));
select dateTime64Diff(toDateTime64('2022-11-23 09:26:37.123456789', 9), toDate('2022-01-01')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select dateTime64Diff(toDateTime64('2022-11-23 09:26:37.123456789', 9), toDate32('2022-01-01')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select dateTime64Diff(toDateTime64('2022-11-23 09:26:37.123456789', 9), toDateTime('2022-01-01 01:02:03')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- DateTime64 - DateTime64
select '-- DateTime64 arithmetic --';
select toDateTime64('2022-11-23 09:26:37.123456789', 9) - toDateTime64('2022-01-01', 0);
