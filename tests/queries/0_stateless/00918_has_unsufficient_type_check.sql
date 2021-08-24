SELECT hasAny([['Hello, world']], [[[]]]);
SELECT hasAny([['Hello, world']], [['Hello', 'world'], ['Hello, world']]);
SELECT hasAll([['Hello, world']], [['Hello', 'world'], ['Hello, world']]);
