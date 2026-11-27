-- array_to_string is a PostgreSQL alias of arrayStringConcat.
SELECT array_to_string([1, 2, 3], ',');
SELECT ARRAY_TO_STRING(['a', 'b', 'c'], '-');
SELECT array_to_string([], '|');
