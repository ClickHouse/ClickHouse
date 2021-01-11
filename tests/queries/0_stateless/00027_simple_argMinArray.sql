SELECT argMinArray(id, num), argMaxArray(id, num)  FROM (SELECT arrayJoin([[10, 4, 3], [7, 5, 6], [8, 8, 2]]) AS num, arrayJoin([[1, 2, 4], [2, 3, 3]]) AS id)
