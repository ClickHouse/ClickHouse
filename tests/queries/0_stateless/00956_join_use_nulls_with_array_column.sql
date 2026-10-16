SET join_use_nulls = 1;
-- The left side is bounded: the outer `LIMIT` counts joined rows, so it is not pushed into a source of a join.
-- Each query is deterministic without `ORDER BY` (e.g. with parallel replicas): only the row `0` matches,
-- and the `Array` column of unmatched rows is filled with the default value, as `Array` cannot be inside `Nullable`.
SELECT number FROM numbers(10) AS t SEMI LEFT JOIN (SELECT number, ['test'] FROM system.numbers LIMIT 1) js2 USING (number) LIMIT 1;
SELECT arr FROM numbers(1, 9) AS t ANY LEFT  JOIN (SELECT number, ['test'] AS arr FROM system.numbers LIMIT 1) js2 USING (number) LIMIT 1;
SELECT count(), countIf(empty(arr)) FROM (SELECT arr FROM numbers(10) AS t ANY LEFT JOIN (SELECT number, ['test'] AS arr FROM system.numbers LIMIT 1) js2 USING (number) LIMIT 10);
