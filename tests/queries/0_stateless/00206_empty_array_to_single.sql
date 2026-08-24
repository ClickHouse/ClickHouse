SELECT emptyArrayToSingle(arrayFilter(x -> x != 99, arrayJoin([[1, 2], [99], [4, 5, 6]])));
SELECT emptyArrayToSingle(emptyArrayString()), emptyArrayToSingle(emptyArrayDate()), emptyArrayToSingle(arrayFilter(x -> 0, [now('Asia/Istanbul')]));

SELECT
    emptyArrayToSingle(range(number % 3)),
    emptyArrayToSingle(arrayMap(x -> toString(x), range(number % 2))),
    emptyArrayToSingle(arrayMap(x -> toDateTime('2015-01-01 00:00:00', 'UTC') + x, range(number % 5))),
    emptyArrayToSingle(arrayMap(x -> toDate('2015-01-01') + x, range(number % 4))) FROM system.numbers LIMIT 10;
