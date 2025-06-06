-- Tags: no-parallel

SELECT arrayMap(multiply(_1, _2) | plus(_, 2), [1, 2, 3], [4, 5, 6]);
SELECT arrayMap(compose(multiply(_1, _2), plus(_, 2)), [1, 2, 3], [4, 5, 6]);
SELECT arrayMap((x -> x * 2) | toString(_), [1, 2, 3]);
SELECT arrayMap((x -> x * 2) | (x -> x * 2), [1, 2, 3]);

SELECT arrayMap(plus(_, 5) | plus(_, 5) | plus(_, 5), [1, 2, 3]);
SELECT arrayMap(plus(_, _) | negate(_), [1, 2, 3], [4, 5, 6]);

SELECT arrayMap(negate(_) | plus(_, _), [1, 2, 3]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayMap(5 | plus(_, 5), [1, 2, 3]);  -- { serverError BAD_ARGUMENTS }
SELECT arrayMap(plus(_, 5) | 5, [1, 2, 3]); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS 03323_tmp_string_table;
CREATE TEMPORARY TABLE 03323_tmp_string_table AS
SELECT 'wwwwwww' AS original_string
FROM numbers(5);

SELECT arrayMap(concat(_1, 'in') | substring(_1, 7), [original_string]) AS mapped_array
FROM 03323_tmp_string_table;

DROP TABLE 03323_tmp_string_table;
