SELECT x FROM (SELECT arrayJoin(['Hello', 'Goodbye']) AS x, [1, 2, 3] AS arr) ARRAY JOIN arr
