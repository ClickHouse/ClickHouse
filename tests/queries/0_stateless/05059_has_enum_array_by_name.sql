-- Searching an array of Enum values for a String matches by the name of the enum value,
-- the same way `equals` does. A constant array used to compare the numeric values of the enum
-- with the string and silently find nothing.

SELECT 'constant array';
WITH CAST(['a', 'c'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3))') AS e
SELECT has(e, 'a'), has(e, 'b'), notHas(e, 'a'), indexOf(e, 'c'), countEqual(e, 'a');

SELECT 'materialized array';
WITH materialize(CAST(['a', 'c'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3))')) AS e
SELECT has(e, 'a'), has(e, 'b'), notHas(e, 'a'), indexOf(e, 'c'), countEqual(e, 'a');

SELECT 'a name that is not in the Enum';
WITH CAST(['a', 'c'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3))') AS e
SELECT has(e, 'zzz'), indexOf(e, 'zzz'), countEqual(e, 'zzz'), e = ['a', 'c'];

SELECT 'a non-constant needle over a constant array';
WITH CAST(['a', 'c'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3))') AS e
SELECT has(e, x), indexOf(e, x) FROM (SELECT arrayJoin(['a', 'b', 'c', 'zzz']) AS x);

SELECT 'the numeric value of the enum still matches';
WITH CAST(['a', 'c'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3))') AS e
SELECT has(e, 1), has(e, 2), indexOf(e, 3);

SELECT 'a constant map with Enum keys';
WITH CAST(map('a', 10, 'c', 30), 'Map(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3), Int64)') AS m
SELECT mapContains(m, 'a'), mapContains(m, 'b'), mapContainsKey(m, 'c'), has(mapKeys(m), 'a');

SELECT 'Enum16 and negative enum values';
WITH CAST(['hello'], 'Array(Enum16(\'hello\' = -300, \'world\' = 500))') AS e
SELECT has(e, 'hello'), has(e, 'world'), indexOf(e, 'hello'), has(e, -300);

SELECT 'a Nullable needle';
WITH CAST(['a', 'c'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3))') AS e
SELECT has(e, CAST('a', 'Nullable(String)')), has(e, CAST(NULL, 'Nullable(String)'));

SELECT 'a LowCardinality needle';
WITH CAST(['a', 'c'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3))') AS e
SELECT has(e, toLowCardinality('a')), has(e, toLowCardinality('b'));

SELECT 'indexOfAssumeSorted';
WITH CAST(['a', 'b', 'c'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3))') AS e
SELECT indexOfAssumeSorted(e, 'a'), indexOfAssumeSorted(e, 'c'), indexOfAssumeSorted(e, 'zzz');

SELECT 'a FixedString needle';
WITH CAST(['a', 'c'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3))') AS e
SELECT has(e, toFixedString('a', 2)), has(e, toFixedString('b', 2)), indexOf(e, toFixedString('c', 4));

SELECT 'the same over a materialized array';
WITH materialize(CAST(['a', 'c'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3))')) AS e
SELECT has(e, toFixedString('a', 2)), has(e, toFixedString('b', 2)), indexOf(e, toFixedString('c', 4));
