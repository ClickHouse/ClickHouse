SELECT sumMap(['a', 'b'], [1, NULL]);
SELECT sumMap(['a', 'b'], [1, toNullable(0)]);
