SELECT hasAny([['Hello, world']], [[[]]]); -- { serverError 386 }
SELECT hasAny([['Hello, world']], [['Hello', 'world'], ['Hello, world']]);
SELECT hasAll([['Hello, world']], [['Hello', 'world'], ['Hello, world']]);
