-- `Time` keys of the `-Map` combinator, both in the `Map` input form and in the separate key argument form.
SET enable_time_time64_type = 1;

SELECT sumMap(map(toTime('00:00:00'), 1));
SELECT sumMap(1, toTime('00:00:00'));
SELECT sumMap(map(CAST(number % 3, 'Time'), number)) FROM numbers(10);
SELECT sumMap(number, CAST(number % 3, 'Time')) FROM numbers(10);
SELECT argMaxMap(number, number, CAST(number % 2, 'Time')) FROM numbers(10);
SELECT toTypeName(sumMap(number, CAST(number % 3, 'Time'))) FROM numbers(10);
SELECT sumMapMerge(state) FROM (SELECT sumMapState(number, CAST(number % 3, 'Time')) AS state FROM numbers(10));
