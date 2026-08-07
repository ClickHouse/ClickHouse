-- https://github.com/ClickHouse/ClickHouse/issues/47552

DROP TABLE IF EXISTS clickhouse_alias_issue_1;
DROP TABLE IF EXISTS clickhouse_alias_issue_2;

CREATE TABLE clickhouse_alias_issue_1 (
    id bigint,
    column_1 Nullable(Float32)
) Engine=Memory;

CREATE TABLE clickhouse_alias_issue_2 (
    id bigint,
    column_2 Nullable(Float32)
) Engine=Memory;

SET enable_analyzer = 1;

INSERT INTO `clickhouse_alias_issue_1`
VALUES (1, 100), (2, 200), (3, 300);

INSERT INTO `clickhouse_alias_issue_2`
VALUES (1, 10), (2, 20), (3, 30);

-- This query returns the expected result
-- 300	\N	3
-- 200	\N	2
-- 100	\N	1
-- \N	30	3
-- \N	20	2
-- \N	10	1
SELECT *
FROM
(
SELECT
  max(`column_1`) AS `column_1`,
NULL AS `column_2`,
  `id`
FROM `clickhouse_alias_issue_1`
GROUP BY
  `id`
UNION ALL
SELECT
  NULL AS `column_1`,
  max(`column_2`) AS `column_2`,
  `id`
FROM `clickhouse_alias_issue_2`
GROUP BY
  `id`
SETTINGS prefer_column_name_to_alias=1
)
ORDER BY ALL DESC NULLS LAST;

SELECT '-------------------------';

-- This query also returns the expected result
-- 300	30	3
-- 200	20	2
-- 100	10	1
SELECT
  max(`column_1`) AS `column_1`,
  max(`column_2`) AS `column_2`,
  `id`
FROM (
  SELECT
    max(`column_1`) AS `column_1`,
    NULL AS `column_2`,
    `id`
  FROM `clickhouse_alias_issue_1`
  GROUP BY
    `id`
  UNION ALL
  SELECT
    NULL AS `column_1`,
    max(`column_2`) AS `column_2`,
    `id`
  FROM `clickhouse_alias_issue_2`
  GROUP BY
    `id`
  SETTINGS prefer_column_name_to_alias=1
) as T1
GROUP BY `id`
ORDER BY `id` DESC
SETTINGS prefer_column_name_to_alias=1;

SELECT '-------------------------';

-- Expected result :
-- 10	3
-- 10	2
-- 10	1
SELECT `column_1` / `column_2`, `id`
FROM (
    SELECT
        max(`column_1`) AS `column_1`,
        max(`column_2`) AS `column_2`,
        `id`
    FROM (
        SELECT
          max(`column_1`) AS `column_1`,
          NULL AS `column_2`,
          `id`
        FROM `clickhouse_alias_issue_1`
        GROUP BY
          `id`
        UNION ALL
        SELECT
          NULL AS `column_1`,
          max(`column_2`) AS `column_2`,
          `id`
        FROM `clickhouse_alias_issue_2`
        GROUP BY
          `id`
        SETTINGS prefer_column_name_to_alias=1
        ) as T1
    GROUP BY `id`
    ORDER BY `id` DESC
    SETTINGS prefer_column_name_to_alias=1
) as T2
WHERE `column_1` IS NOT NULL AND `column_2` IS NOT NULL
SETTINGS prefer_column_name_to_alias=1;

SELECT '-------------------------';

-- Without the setting, the expected result is the same
-- but the actual result isn't wrong
SELECT `column_1` / `column_2`, `id`
FROM (
    SELECT
        max(`column_1`) AS `column_1`,
        max(`column_2`) AS `column_2`,
        `id`
    FROM (
        SELECT
          max(`column_1`) AS `column_1`,
          NULL AS `column_2`,
          `id`
        FROM `clickhouse_alias_issue_1`
        GROUP BY
          `id`
        UNION ALL
        SELECT
          NULL AS `column_1`,
          max(`column_2`) AS `column_2`,
          `id`
        FROM `clickhouse_alias_issue_2`
        GROUP BY
          `id`
        ) as T1
    GROUP BY `id`
    ORDER BY `id` DESC
) as T2
WHERE `column_1` IS NOT NULL AND `column_2` IS NOT NULL;

DROP TABLE IF EXISTS clickhouse_alias_issue_1;
DROP TABLE IF EXISTS clickhouse_alias_issue_2;
