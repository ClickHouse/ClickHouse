-- https://github.com/ClickHouse/ClickHouse/pull/110206
-- `GROUP BY ALL` expands the SELECT expressions into grouping keys after the explicit `GROUP BY`
-- key-validation path already ran, so it must redo `validateGroupByKeyType` on the expanded keys
-- itself -- otherwise a suspicious key type such as `Dynamic`/`Variant`, which an explicit
-- `GROUP BY` rejects, would be silently accepted by `GROUP BY ALL`.

SET enable_analyzer = 1;

SELECT d FROM (SELECT 1::Dynamic AS d) GROUP BY ALL SETTINGS allow_suspicious_types_in_group_by = 0; -- { serverError ILLEGAL_COLUMN }
SELECT d FROM (SELECT 1::Dynamic AS d) GROUP BY d SETTINGS allow_suspicious_types_in_group_by = 0; -- { serverError ILLEGAL_COLUMN }

-- Same check for a `Dynamic` element nested inside a tuple grouping key.
SELECT tuple(d, 1) FROM (SELECT 1::Dynamic AS d) GROUP BY ALL SETTINGS allow_suspicious_types_in_group_by = 0; -- { serverError ILLEGAL_COLUMN }
SELECT tuple(d, 1) FROM (SELECT 1::Dynamic AS d) GROUP BY tuple(d, 1) SETTINGS allow_suspicious_types_in_group_by = 0; -- { serverError ILLEGAL_COLUMN }

-- Allowed once the setting permits suspicious types.
SELECT d FROM (SELECT 1::Dynamic AS d) GROUP BY ALL SETTINGS allow_suspicious_types_in_group_by = 1;
SELECT tuple(d, 1) FROM (SELECT 1::Dynamic AS d) GROUP BY ALL SETTINGS allow_suspicious_types_in_group_by = 1;

-- `validate_group_by_all_key_types` gates the key types that `GROUP BY ALL` expands into, and only
-- those: with it off `GROUP BY ALL` accepts a suspicious key type again while an explicit `GROUP BY`
-- keeps rejecting it, which is what distinguishes this setting from `allow_suspicious_types_in_group_by`.
SELECT d FROM (SELECT 1::Dynamic AS d) GROUP BY ALL SETTINGS allow_suspicious_types_in_group_by = 0, validate_group_by_all_key_types = 0;
SELECT d FROM (SELECT 1::Dynamic AS d) GROUP BY d SETTINGS allow_suspicious_types_in_group_by = 0, validate_group_by_all_key_types = 0; -- { serverError ILLEGAL_COLUMN }

-- The tuple expansion is not gated, so a tuple grouping key is still unwrapped into its elements with
-- the validation off and an `ORDER BY` of the same tuple still finds them in the aggregated block.
SELECT tuple(c0, c1) AS t FROM (SELECT 1 c0, 2 c1) v0 GROUP BY ALL ORDER BY t
    SETTINGS validate_group_by_all_key_types = 0, optimize_injective_functions_in_group_by = 0;

-- Under `group_by_use_nulls` with a `WITH ROLLUP` modifier the `GROUP BY ALL` keys are expanded before
-- they are resolved, so they are validated on the same path as an explicit `GROUP BY`. The setting
-- reaches that path too, while an explicit `GROUP BY` there stays unconditional. The second row of the
-- accepted arm is the rollup total, whose key is NULL because `group_by_use_nulls` promoted it.
SELECT d FROM (SELECT 1::Dynamic AS d) GROUP BY ALL WITH ROLLUP SETTINGS group_by_use_nulls = 1, allow_suspicious_types_in_group_by = 0; -- { serverError ILLEGAL_COLUMN }
SELECT d FROM (SELECT 1::Dynamic AS d) GROUP BY ALL WITH ROLLUP SETTINGS group_by_use_nulls = 1, allow_suspicious_types_in_group_by = 0, validate_group_by_all_key_types = 0;
SELECT d FROM (SELECT 1::Dynamic AS d) GROUP BY d WITH ROLLUP SETTINGS group_by_use_nulls = 1, allow_suspicious_types_in_group_by = 0, validate_group_by_all_key_types = 0; -- { serverError ILLEGAL_COLUMN }

-- `compatibility` with a version before 26.7 restores the earlier acceptance; with 26.7 itself it does
-- not, because 26.7 is the version that started rejecting such a key.
SELECT d FROM (SELECT 1::Dynamic AS d) GROUP BY ALL SETTINGS allow_suspicious_types_in_group_by = 0, compatibility = '26.6';
SELECT d FROM (SELECT 1::Dynamic AS d) GROUP BY ALL SETTINGS allow_suspicious_types_in_group_by = 0, compatibility = '26.7'; -- { serverError ILLEGAL_COLUMN }

-- The reported shape: an untyped JSON subpath is a `Dynamic` grouping key, so it is gated the same way.
SELECT c1.p1 FROM (SELECT '{"p1":"v0"}'::JSON AS c1) GROUP BY ALL SETTINGS allow_suspicious_types_in_group_by = 0, validate_group_by_all_key_types = 0;
SELECT c1.p1 FROM (SELECT '{"p1":"v0"}'::JSON AS c1) GROUP BY c1.p1 SETTINGS allow_suspicious_types_in_group_by = 0, validate_group_by_all_key_types = 0; -- { serverError ILLEGAL_COLUMN }
