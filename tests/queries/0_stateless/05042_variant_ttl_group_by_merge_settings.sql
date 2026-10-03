-- The aggregates of `TTL ... GROUP BY ... SET` (and the implicit `any` of the uncovered columns) are cached in
-- the table metadata with the settings that were active at `CREATE`/`ATTACH` time. The merge that executes them
-- must re-resolve them with its own settings: `aggregate_functions_skip_variant_nulls` belongs to the query that
-- runs `OPTIMIZE` (whose settings the merge preserves), not to the session that created the table.

SET allow_experimental_variant_type = 1;

DROP TABLE IF EXISTS t_variant_ttl_group_by;

-- Create the table under the compatibility value of the setting: if the cached resolution were used by the
-- merge, every `OPTIMIZE` below would keep the NULLs regardless of its own settings.
SET aggregate_functions_skip_variant_nulls = 0;

CREATE TABLE t_variant_ttl_group_by
(
    k UInt64,
    d DateTime,
    v Variant(String),
    u Variant(String)
)
ENGINE = MergeTree
ORDER BY (k, d)
TTL d + INTERVAL 1 SECOND GROUP BY k SET v = any(v)
-- `max_number_of_merges_with_ttl_in_pool = 0` disables background TTL merges for the table: otherwise the
-- background pool could apply the TTL (with its own default settings) before `OPTIMIZE` gets a chance to, and
-- the result would depend on that race. `merge_with_ttl_timeout` alone is not enough, because it only spaces
-- out TTL merges after the first one. `OPTIMIZE` does not go through the merge selector, so it is unaffected.
SETTINGS merge_with_ttl_timeout = 100000, max_number_of_merges_with_ttl_in_pool = 0;

SET aggregate_functions_skip_variant_nulls = 1;

-- A single INSERT, so that the two rows land in one part and no ordinary background merge can consume them
-- ahead of `OPTIMIZE` either. `d` is part of the sorting key and the NULL row has the smaller value, so the
-- stream always presents it first and `any` picks it up unless the NULL-skipping is in effect. (Sharing a
-- single sorting key value between the two rows would leave the order up to the tie-breaking of the merging
-- algorithm, which is not stable.) `u` is aggregated by the implicit uncovered-column `any`.
INSERT INTO t_variant_ttl_group_by VALUES (1, '2020-01-01 00:00:00', NULL, NULL), (1, '2020-01-01 00:00:01', 'x', 'y');

-- The session setting (skip = 1) applies to the OPTIMIZE query, overriding the CREATE-time value.
OPTIMIZE TABLE t_variant_ttl_group_by FINAL;
SELECT 'skip nulls', v, u FROM t_variant_ttl_group_by;

TRUNCATE TABLE t_variant_ttl_group_by;
INSERT INTO t_variant_ttl_group_by VALUES (1, '2020-01-01 00:00:00', NULL, NULL), (1, '2020-01-01 00:00:01', 'x', 'y');

-- And the explicit per-query value takes precedence over the session one.
OPTIMIZE TABLE t_variant_ttl_group_by FINAL SETTINGS aggregate_functions_skip_variant_nulls = 0;
SELECT 'keep nulls', v, u FROM t_variant_ttl_group_by;

DROP TABLE t_variant_ttl_group_by;
