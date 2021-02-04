select deltaSum(arrayJoin([1, 2, 3]));
select deltaSum(arrayJoin([1, 2, 3, 0, 3, 4]));
select deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]));
select deltaSum(arrayJoin([1, 2, 3, 0, 3, 3, 3, 3, 3, 4, 2, 3]));
select deltaSum(arrayJoin([1, 2, 3, 0, 0, 0, 0, 3, 3, 3, 3, 3, 4, 2, 3]));
