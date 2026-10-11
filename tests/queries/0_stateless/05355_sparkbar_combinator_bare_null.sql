-- A bare `NULL` in the key or in a forwarded argument of a `-Sparkbar` function skips every row,
-- so the result is the empty sparkbar `String`, whatever the nested function's null semantics are.

-- `avg` does not return a default for an all-`NULL` input, `count` and `uniq` do.
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbar(3, 0, 2)(number, NULL) AS r FROM numbers(3));
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbar(3, 0, 2)(NULL, number) AS r FROM numbers(3));
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT sumSparkbar(3, 0, 2)(number, NULL) AS r FROM numbers(3));
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT countSparkbar(3, 0, 2)(NULL) AS r FROM numbers(3));
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT countSparkbar(3, 0, 2)(number, NULL) AS r FROM numbers(3));
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT uniqSparkbar(3, 0, 2)(NULL, NULL) AS r FROM numbers(3));

-- `anyRespectNulls` is not wrapped into the `Null` combinator, the combinator handles the `NULL` key itself.
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT anyRespectNullsSparkbar(3, 0, 2)(NULL, number) AS r FROM numbers(3));
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT anyRespectNullsSparkbar(3, 0, 2)(number, NULL) AS r FROM numbers(3));

-- With groups.
SELECT number % 2 AS k, concat('[', avgSparkbar(3, 0, 2)(number, NULL), ']') FROM numbers(6) GROUP BY k ORDER BY k;

-- A `Nullable` argument that is not a bare `NULL` still renders.
SELECT concat('[', avgSparkbar(3, 0, 2)(number, toNullable(number)), ']') FROM numbers(3);
