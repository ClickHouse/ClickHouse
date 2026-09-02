SELECT cityHash64('abc') IN cityHash64('abc');
SELECT cityHash64(arrayJoin(['abc', 'def'])) IN cityHash64('abc');
