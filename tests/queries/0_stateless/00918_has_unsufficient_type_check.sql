SELECT hasAny([['Hello, world']], [[[]]]); -- { serverError NO_COMMON_TYPE }
SELECT hasAny([['Hello, world']], [['Hello', 'world'], ['Hello, world']]);
SELECT hasAll([['Hello, world']], [['Hello', 'world'], ['Hello, world']]);
