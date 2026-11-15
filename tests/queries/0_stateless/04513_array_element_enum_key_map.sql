-- Element access of a Map with Enum keys by the name of the enum value.

-- Constant index; a key that is in the Enum but not in the map reads as the default value.
WITH CAST(map('a', 1, 'c', 3), 'Map(Enum8(''a'' = 1, ''b'' = 2, ''c'' = 3), UInt8)') AS m
SELECT m['a'], m['b'], m['c'];

-- Enum16 and arbitrary enum values.
WITH CAST(map('hello', 1, 'world', 2), 'Map(Enum16(''hello'' = -300, ''world'' = 500), Int64)') AS m
SELECT m['hello'], m['world'];

-- Access by the numeric value of the enum still works.
WITH CAST(map('a', 5), 'Map(Enum8(''a'' = 1, ''b'' = 2), UInt8)') AS m
SELECT m[1], m[2];

-- Non-constant string index.
SELECT CAST(map('a', number, 'b', number * 2), 'Map(Enum8(''a'' = 1, ''b'' = 2), UInt64)')[materialize(if(number % 2 = 0, 'a', 'b'))] FROM numbers(4);

-- LowCardinality(String) index.
WITH CAST(map('a', 1), 'Map(Enum8(''a'' = 1, ''b'' = 2), UInt8)') AS m
SELECT m[toLowCardinality('a')], m[toLowCardinality('b')];

-- arrayElementOrNull returns NULL for a key that is absent from the map.
WITH CAST(map('a', 1, 'c', 3), 'Map(Enum8(''a'' = 1, ''b'' = 2, ''c'' = 3), UInt8)') AS m
SELECT arrayElementOrNull(m, 'a'), arrayElementOrNull(m, 'b');

-- A name that is not in the Enum is an error.
WITH CAST(map('a', 1), 'Map(Enum8(''a'' = 1), UInt8)') AS m SELECT m['b']; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
