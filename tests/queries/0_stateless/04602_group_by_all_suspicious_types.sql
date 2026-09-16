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
