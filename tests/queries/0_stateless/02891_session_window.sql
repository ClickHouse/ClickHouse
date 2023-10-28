-- { echoOn }
select arrayJoin([1, 20, 21, 22, 100, 101]) n, groupArray(n) over (order by n session 0);
select arrayJoin([1, 20, 21, 22, 100, 101]) n, groupArray(n) over (order by n session 1);
select arrayJoin([1, 20, 21, 22, 100, 101]) n, groupArray(n) over (order by n session 100);
select arrayJoin([1, 20, 21, 22, 100, 101]) n, groupArray(n) over (order by n desc session 1);
select arrayJoin([1, 20, 21, 22, 100, 101]) n, groupArray(n) over (order by n desc session 100);

