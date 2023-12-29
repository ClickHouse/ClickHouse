WITH arrayJoin(['Hello', 'world'])::Enum('Hello', 'world') AS x SELECT x, transform(x, ['Hello', 'world'], [123, 456], 0);
WITH arrayJoin(['Hello', 'world'])::Enum('Hello', 'world') AS x SELECT x, transform(x, ['Hello', 'world', 'goodbye'], [123, 456], 0); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
