SET allow_materialized_view_with_bad_select = 1;

DROP TABLE IF EXISTS mv_extra_columns_dst;
DROP TABLE IF EXISTS mv_extra_columns_src;
DROP TABLE IF EXISTS mv_extra_columns_view;

CREATE TABLE mv_extra_columns_dst (
    v UInt64
) ENGINE = MergeTree()
    PARTITION BY tuple()
    ORDER BY v;

CREATE TABLE mv_extra_columns_src (
    v1 UInt64,
    v2 UInt64
) ENGINE = Null;

-- Extra columns are ignored when pushing to destination table.
-- This test exists to prevent unintended changes to existing behaviour.
--
-- Although this behaviour might not be ideal it can be exploited for 0-downtime changes to materialized views.
-- Step 1: Add new column to source table. Step 2: Create new view reading source column.
-- Step 3: Swap views using `RENAME TABLE`. Step 4: Add new column to destination table as well.
CREATE MATERIALIZED VIEW mv_extra_columns_view TO mv_extra_columns_dst
AS SELECT
  v1 as v,
  v2 as v2
FROM mv_extra_columns_src;

INSERT INTO mv_extra_columns_src VALUES (0, 0), (1, 1), (2, 2);

SELECT * FROM mv_extra_columns_dst ORDER by v;
SELECT * FROM mv_extra_columns_view; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

DROP TABLE mv_extra_columns_view;
DROP TABLE mv_extra_columns_src;
DROP TABLE mv_extra_columns_dst;
