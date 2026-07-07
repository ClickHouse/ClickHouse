-- { echoOn }

-- The SESSION frame is experimental and disabled by default.
select 1 n, count() over (order by n session 1); -- { serverError SUPPORT_IS_DISABLED }

set allow_experimental_session_window_frame = 1;

select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n session 1);
select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n session 2);
select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n session 100);
select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n desc session 2);
select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n desc session 100);

-- Fractional session window thresholds are also useful, e.g. to process bursts of events occurring less than 0.5 second apart.
select arrayJoin([1, 2.0, 2.1, 2.2, 10.0, 10.1])::float n, groupArray(n) over (order by n session 0.5);

-- PARTITION BY: sessions are formed independently within each partition.
select p, n, groupArray(n) over (partition by p order by n session 2)
from (select arrayJoin([1, 2, 10, 11]) n, arrayJoin(['a', 'b']) p)
order by p, n;

-- DateTime ORDER BY key: the threshold is in seconds (RANGE OFFSET units), so
-- SESSION 60 groups events at most 60 seconds apart.
select t, groupArray(t) over (order by t session 60)
from (select arrayJoin([
    toDateTime('2020-01-01 00:00:00'),
    toDateTime('2020-01-01 00:00:30'),
    toDateTime('2020-01-01 00:01:15'),
    toDateTime('2020-01-01 00:05:00')]) t)
order by t;

-- Test some wrong things
select 1 n, count() over (order by n session 0.5); -- { serverError 69 }
select 1 n, count() over (order by n session -1); -- { serverError 69 }
select 1 n, count() over (order by n session 0); -- { serverError 36 }
select 1 n, count() over (order by n session 'what'); -- { serverError BAD_ARGUMENTS }
select 1 n, count() over (session 1); -- { serverError 36 }
select 1 n, count() over (order by n, n+1 session 1); -- { serverError 36 }
select 'a' n, count() over (order by n session 1); -- { serverError 48 }
