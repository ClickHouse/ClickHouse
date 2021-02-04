select deltaSum(arrayJoin([1, 2, 3]));
select deltaSum(arrayJoin([1, 2, 3, 0, 3, 4]));
select deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]));
select deltaSum(arrayJoin([1, 2, 3, 0, 3, 3, 3, 3, 3, 4, 2, 3]));
select deltaSum(arrayJoin([1, 2, 3, 0, 0, 0, 0, 3, 3, 3, 3, 3, 4, 2, 3]));
select deltaSumMerge(rows) from (select deltaSumState(arrayJoin([0, 1])) as rows union all select deltaSumState(arrayJoin([4, 5])) as rows);
select deltaSumMerge(rows) from (select deltaSumState(arrayJoin([4, 5])) as rows union all select deltaSumState(arrayJoin([0, 1])) as rows);
