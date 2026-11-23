SELECT emptyArrayToSingle(arrayFilter(x -> x != 99, arrayJoin([[1, 2], [99], [4, 5, 6]])));
SELECT emptyArrayToSingle(emptyArrayString()), emptyArrayToSingle(emptyArrayDate()), emptyArrayToSingle(arrayFilter(x -> 0, [now('Asia/Istanbul')]));

SELECT
    emptyArrayToSingle(range(number % 3)),
    emptyArrayToSingle(arrayMap(x -> toString(x), range(number % 2))),
    emptyArrayToSingle(arrayMap(x -> toDateTime('2015-01-01 00:00:00', 'UTC') + x, range(number % 5))),
    emptyArrayToSingle(arrayMap(x -> toDate('2015-01-01') + x, range(number % 4))) FROM system.numbers LIMIT 10;

SELECT emptyArrayToSingle(materialize([1, 2])), emptyArrayToSingle(materialize(['a', ''])), emptyArrayToSingle(materialize([toNullable(1), NULL])), emptyArrayToSingle(materialize([toFixedString('ab', 2)])), emptyArrayToSingle(materialize([toLowCardinality('x')])), emptyArrayToSingle(materialize([[], [1]]));
SELECT number, emptyArrayToSingle(range(number % 3 + 1)), emptyArrayToSingle(if(number = 3, [], [toString(number)])) FROM numbers(4) ORDER BY number;
SELECT x, y FROM (SELECT [number] AS a, if(number = 1, [], [number * 10]) AS b FROM numbers(3)) LEFT ARRAY JOIN a AS x, b AS y ORDER BY x;
SELECT kv FROM (SELECT map('a', number) AS m FROM numbers(2)) LEFT ARRAY JOIN m AS kv ORDER BY kv;
