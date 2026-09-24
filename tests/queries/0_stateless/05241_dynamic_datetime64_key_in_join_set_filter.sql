-- A set lookup converts the key to the key type of the set and excludes the rows whose `DateTime64` value
-- does not survive the conversion. A `Dynamic` key is never converted this way. `IN` rejects a left operand
-- with a dynamic structure, and a join casts both of its keys to their least supertype, which is `Dynamic`
-- whenever one side is `Dynamic`, before the sets built for `max_rows_in_set_to_optimize_join` filter the
-- other side.

SELECT materialize(toDateTime64('2026-01-01 00:00:00.5', 9, 'UTC'))::Dynamic
    IN (SELECT toDateTime('2026-01-01 00:00:00', 'UTC')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT materialize(toDateTime64('2026-01-01 00:00:00.5', 9, 'UTC'))::Dynamic
    IN (toFloat64(1767225600.5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SET explain_query_plan_default = 'legacy';
SET join_algorithm = 'full_sorting_merge';
SET max_rows_in_set_to_optimize_join = 100000;
SET allow_dynamic_type_in_join_keys = 1;

-- The key types of both set filters, which are the types their set lookups receive.
SELECT arrayJoin(arrayMap(
    line -> extract(line, '\\S+$'),
    arrayFilter((line, i) -> i > 1 AND lines[i - 1] LIKE '%CreateSetAndFilterOnTheFlyStep (%', lines, arrayEnumerate(lines))))
FROM
(
    SELECT groupArray(explain) AS lines
    FROM
    (
        EXPLAIN header = 1
        SELECT count(l.k)
        FROM (SELECT materialize(toDateTime64('2026-01-01 00:00:00.5', 9, 'UTC'))::Dynamic AS k FROM numbers(3)) AS l
        INNER JOIN (SELECT materialize(toDateTime('2026-01-01 00:00:00', 'UTC')) AS k FROM numbers(3)) AS r ON l.k = r.k
    )
);

-- Each `DateTime64(9)` value below loses precision in the type of the other key: `DateTime` drops the half
-- second, `Float64` rounds the nanoseconds and `DateTime64(3)` drops everything below a millisecond. The
-- value matches none of them, also when the `Dynamic` column stores it in its shared variant.
SELECT count(l.k)
FROM (SELECT materialize(toDateTime64('2026-01-01 00:00:00.5', 9, 'UTC'))::Dynamic AS k FROM numbers(3)) AS l
INNER JOIN (SELECT materialize(toDateTime('2026-01-01 00:00:00', 'UTC')) AS k FROM numbers(3)) AS r ON l.k = r.k;

SELECT count(l.k)
FROM (SELECT materialize(toDateTime64('2026-01-01 00:00:00.123456789', 9, 'UTC'))::Dynamic AS k FROM numbers(3)) AS l
INNER JOIN
(
    SELECT toFloat64(materialize(toDateTime64('2026-01-01 00:00:00.123456789', 9, 'UTC'))) AS k FROM numbers(3)
) AS r ON l.k = r.k;

SELECT count(l.k)
FROM (SELECT materialize(toDateTime64('2026-01-01 00:00:00.123456789', 9, 'UTC'))::Dynamic AS k FROM numbers(3)) AS l
INNER JOIN (SELECT materialize(toDateTime64('2026-01-01 00:00:00.123', 3, 'UTC')) AS k FROM numbers(3)) AS r ON l.k = r.k;

SELECT count(l.k)
FROM
(
    SELECT materialize(toDateTime64('2026-01-01 00:00:00.123456789', 9, 'UTC'))::Dynamic(max_types = 0) AS k
    FROM numbers(3)
) AS l
INNER JOIN (SELECT materialize(toDateTime64('2026-01-01 00:00:00.123', 3, 'UTC')) AS k FROM numbers(3)) AS r ON l.k = r.k;

-- An equal value of the same type matches, whether the `Dynamic` column stores it in a variant of its own or
-- in its shared variant.
SELECT count(l.k)
FROM (SELECT materialize(toDateTime('2026-01-01 00:00:00', 'UTC'))::Dynamic AS k FROM numbers(3)) AS l
INNER JOIN (SELECT materialize(toDateTime('2026-01-01 00:00:00', 'UTC')) AS k FROM numbers(3)) AS r ON l.k = r.k;

SELECT count(l.k)
FROM
(
    SELECT materialize(toDateTime64('2026-01-01 00:00:00.123', 3, 'UTC'))::Dynamic(max_types = 0) AS k
    FROM numbers(3)
) AS l
INNER JOIN (SELECT materialize(toDateTime64('2026-01-01 00:00:00.123', 3, 'UTC')) AS k FROM numbers(3)) AS r ON l.k = r.k;
