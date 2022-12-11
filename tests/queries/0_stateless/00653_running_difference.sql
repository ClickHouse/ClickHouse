select runningDifference(x) from (select arrayJoin([0, 1, 5, 10]) as x);
select '-';
select runningDifference(x) from (select arrayJoin([2, Null, 3, Null, 10]) as x);
select '-';
select runningDifference(x) from (select arrayJoin([Null, 1]) as x);
select '-';
select runningDifference(x) from (select arrayJoin([Null, Null, 1, 3, Null, Null, 5]) as x);

