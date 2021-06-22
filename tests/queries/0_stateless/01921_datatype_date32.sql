drop table if exists t1;
create table t1(x1 Date32) engine Memory;

insert into t1 values
      ('1925-01-01'),
      ('1924-01-01'),
      ('2282-12-31'),
      ('2283-12-31'),
      ('2021-6-22');

select x1 from t1;
select '-------toYear---------';
select toYear(x1) from t1;
select '-------toMonth---------';
select toMonth(x1) from t1;
select '-------toQuarter---------';
select toQuarter(x1) from t1;
select '-------toDayOfMonth---------';
select toDayOfMonth(x1) from t1;
select '-------toDayOfWeek---------';
select toDayOfWeek(x1) from t1;
select '-------toDayOfYear---------';
select toDayOfYear(x1) from t1;
select '-------toHour---------';
select toHour(x1) from t1; -- { serverError 43 }
select '-------toMinute---------';
select toMinute(x1) from t1; -- { serverError 43 }
select '-------toSecond---------';
select toSecond(x1) from t1; -- { serverError 43 }
select '-------toStartOfDay---------';
select toStartOfDay(x1) from t1;
select '-------toMonday---------';
select toMonday(x1) from t1;
select '-------toISOWeek---------';
select toISOWeek(x1) from t1;
select '-------toISOWeek---------';
select toISOYear(x1) from t1;
select '-------toWeek---------';
select toWeek(x1) from t1;
select '-------toYearWeek---------';
select toYearWeek(x1) from t1;
select '-------toStartOfWeek---------';
select toStartOfWeek(x1) from t1;
select '-------toStartOfMonth---------';
select toStartOfMonth(x1) from t1;
select '-------toStartOfQuarter---------';
select toStartOfQuarter(x1) from t1;
select '-------toStartOfYear---------';
select toStartOfYear(x1) from t1;
select '-------toStartOfSecond---------';
select toStartOfSecond(x1) from t1; -- { serverError 43 }
select '-------toStartOfMinute---------';
select toStartOfMinute(x1) from t1; -- { serverError 43 }
select '-------toStartOfFiveMinute---------';
select toStartOfFiveMinute(x1) from t1; -- { serverError 43 }
select '-------toStartOfTenMinutes---------';
select toStartOfTenMinutes(x1) from t1; -- { serverError 43 }
select '-------toStartOfFifteenMinutes---------';
select toStartOfFifteenMinutes(x1) from t1; -- { serverError 43 }
select '-------toStartOfHour---------';
select toStartOfHour(x1) from t1; -- { serverError 43 }
select '-------toStartOfISOYear---------';
select toStartOfISOYear(x1) from t1;
select '-------toRelativeYearNum---------';
select toRelativeYearNum(x1) from t1;
select '-------toRelativeQuarterNum---------';
select toRelativeQuarterNum(x1) from t1;
select '-------toRelativeMonthNum---------';
select toRelativeMonthNum(x1) from t1;
select '-------toRelativeWeekNum---------';
select toRelativeWeekNum(x1) from t1;
select '-------toRelativeDayNum---------';
select toRelativeDayNum(x1) from t1;
select '-------toRelativeHourNum---------';
select toRelativeHourNum(x1) from t1; -- { serverError 43 }
select '-------toRelativeMinuteNum---------';
select toRelativeMinuteNum(x1) from t1; -- { serverError 43 }
select '-------toRelativeSecondNum---------';
select toRelativeSecondNum(x1) from t1; -- { serverError 43 }
select '-------toTime---------';
select toTime(x1) from t1;

