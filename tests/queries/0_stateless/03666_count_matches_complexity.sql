SELECT countMatches(repeat('\0\0\0\0\0\0\0\0\0\0', 1000000), 'a');
SELECT countMatches(repeat('\0\0\0\0\0\0\0\0\0\0a', 1000000), 'a');
