-- `group_by_use_nulls_with_totals` controls whether `group_by_use_nulls` applies to `WITH TOTALS`.
-- Disabling it (directly or through `compatibility`) restores the previous behavior: the keys keep
-- their types and the totals row contains default values.

SELECT number AS k, toTypeName(k), k IS NULL, count()
FROM numbers(3)
GROUP BY k
    WITH TOTALS
ORDER BY k
SETTINGS group_by_use_nulls = 1, group_by_use_nulls_with_totals = 1;

SELECT number AS k, toTypeName(k), k IS NULL, count()
FROM numbers(3)
GROUP BY k
    WITH TOTALS
ORDER BY k
SETTINGS group_by_use_nulls = 1, group_by_use_nulls_with_totals = 0;

SELECT number AS k, toTypeName(k), k IS NULL, count()
FROM numbers(3)
GROUP BY k
    WITH TOTALS
ORDER BY k
SETTINGS group_by_use_nulls = 1, compatibility = '26.9';

-- With ROLLUP, the keys are Nullable regardless, but the totals row is NULL only with the new setting.
SELECT number % 2 AS k, count()
FROM numbers(4)
GROUP BY k
    WITH ROLLUP WITH TOTALS
ORDER BY k
SETTINGS group_by_use_nulls = 1, group_by_use_nulls_with_totals = 0;

SELECT number % 2 AS k, count()
FROM numbers(4)
GROUP BY k
    WITH ROLLUP WITH TOTALS
ORDER BY k
SETTINGS group_by_use_nulls = 1, group_by_use_nulls_with_totals = 1;
