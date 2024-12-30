-- { echoOn }

select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n session 1);
select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n session 2);
select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n session 100);
select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n desc session 2);
select arrayJoin([1, 20, 22, 24, 100, 101]) n, groupArray(n) over (order by n desc session 100);

-- Fractional session window thresholds are also useful, e.g. to process bursts of events occurring less than 0.5 second apart.
select arrayJoin([1, 2.0, 2.1, 2.2, 10.0, 10.1])::float n, groupArray(n) over (order by n session 0.5);

-- Test some wrong things
select 1 n, count() over (order by n session 0.5); -- { serverError 69 }
select 1 n, count() over (order by n session -1); -- { serverError 69 }
select 1 n, count() over (order by n session 0); -- { serverError 36 }
select 1 n, count() over (order by n session 'what'); -- { serverError BAD_ARGUMENTS }
select 1 n, count() over (session 1); -- { serverError 36 }
select 1 n, count() over (order by n, n+1 session 1); -- { serverError 36 }
select 'a' n, count() over (order by n session 1); -- { serverError 48 }

