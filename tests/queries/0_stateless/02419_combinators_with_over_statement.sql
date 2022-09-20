select uniqStateMap(map(1, number)) OVER (PARTITION BY number % 2) from numbers(3);
select uniqStateForEach(range(number)) OVER (PARTITION BY number % 2) from numbers(3);
select uniqStateResample(30, 75, 30)(number, 30) OVER (PARTITION BY number % 2) from numbers(3);
select uniqStateForEachMap(map(1, range(number))) OVER (PARTITION BY number % 2) from numbers(3);
select uniqStateForEachResample(30, 75, 30)(range(number), 30) OVER (PARTITION BY number % 2) from numbers(3);
select uniqStateForEachMapResample(30, 75, 30)(map(1, range(number)), 30) OVER (PARTITION BY number % 2) from numbers(3);
select uniqStateForEachMapForEachMap(map(1, [map(1, range(number))])) from numbers(3);

