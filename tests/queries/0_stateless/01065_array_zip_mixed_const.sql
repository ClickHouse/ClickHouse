SELECT arrayZip([0, 1], ['hello', 'world']);
SELECT arrayZip(materialize([0, 1]), ['hello', 'world']);
SELECT arrayZip([0, 1], materialize(['hello', 'world']));
SELECT arrayZip(materialize([0, 1]), materialize(['hello', 'world']));

SELECT arrayZip([0, number], [toString(number), 'world']) FROM numbers(10);
SELECT arrayZip([1, number, number * number], [[], [], []]) FROM numbers(10);
