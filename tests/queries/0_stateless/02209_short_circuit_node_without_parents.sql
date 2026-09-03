SELECT 1 FROM (SELECT arrayJoin(if(empty(range(number)), [1], [2])) from numbers(1));

