SET group_by_two_level_threshold = 1, max_threads = 1;

SELECT
    k,
    anyLast(s)
FROM
(
    SELECT
        123456789 AS k,
        'Hello 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890' AS s
    UNION ALL
    SELECT
        234567890,
        'World 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890'
)
GROUP BY k
    WITH TOTALS
HAVING length(anyLast(s)) > 0
ORDER BY k;

/* There was a bug in implementation of WITH TOTALS.
 * When there was more than one block after aggregation,
 *  nullptr is passed to IAggregateFunction::merge instead of pointer to valid Arena.
 *
 * To reproduce, we set 'group_by_two_level_threshold' to small value to enable two-level aggregation.
 * Only in two-level aggregation there are many blocks after GROUP BY.
 *
 * Also use UNION ALL in subquery to generate two blocks before GROUP BY.
 * Because two-level aggregation may be triggered only after a block is processed.
 *
 * Use large numbers as a key, because for 8, 16 bit numbers,
 *  two-level aggregation is not possible as simple aggregation method is used.
 * These numbers are happy to hash to different buckets and we thus we have two blocks after GROUP BY.
 *
 * Also we use long strings (at least 64 bytes) in aggregation state,
 *  because aggregate functions min/max/any/anyLast use Arena only for long enough strings.
 *
 * And we use function 'anyLast' for method IAggregateFunction::merge to be called for every new value.
 *
 * We use useless HAVING (that is always true), because in absense of HAVING,
 *  TOTALS are calculated in a simple way in same pass during aggregation, not in TotalsHavingTransform,
 *  and bug doesn't trigger.
 *
 * We use ORDER BY for result of the test to be deterministic.
 * max_threads = 1 for deterministic order of result in subquery and the value of 'anyLast'.
 */
