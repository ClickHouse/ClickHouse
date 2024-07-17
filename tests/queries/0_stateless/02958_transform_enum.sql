WITH arrayJoin(['Hello', 'world'])::Enum('Hello', 'world') AS x SELECT x, transform(x, ['Hello', 'world'], [123, 456], 0);
WITH arrayJoin(['Hello', 'world'])::Enum('Hello', 'world') AS x SELECT x, transform(x, ['Hello', 'world', 'goodbye'], [123, 456], 0); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
WITH arrayJoin(['Hello', 'world'])::Enum('Hello', 'world') AS x SELECT x, transform(x, ['Hello', 'world'], ['test', 'best']::Array(Enum('test' = 123, 'best' = 456, '' = 0)), ''::Enum('test' = 123, 'best' = 456, '' = 0)) AS y;
