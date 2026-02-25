SELECT arrayFilter((a) -> ((a, arrayJoin([])) IN (Null, [Null])), []);
SELECT arrayFilter((a) -> ((a, arrayJoin([[]])) IN (Null, [Null])), []);

SELECT * FROM system.one ARRAY JOIN arrayFilter((a) -> ((a, arrayJoin([])) IN (NULL)), []) AS arr_x; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * FROM numbers(1) LEFT ARRAY JOIN arrayFilter((x_0, x_1) -> (arrayJoin([]) IN (NULL)), [], []) AS arr_x;
