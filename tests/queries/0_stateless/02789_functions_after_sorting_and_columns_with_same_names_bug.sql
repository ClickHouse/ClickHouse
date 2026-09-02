drop table if exists test;
drop table if exists test1;

CREATE TABLE test
(
    `pt` String,
    `count_distinct_exposure_uv` AggregateFunction(uniqHLL12, Int64)
)
ENGINE = AggregatingMergeTree
ORDER BY pt;

SELECT  *
FROM
(
        SELECT  m0.pt                                                                                                                   AS pt
               ,m0.`exposure_uv`                                                                                                        AS exposure_uv
               ,round(m2.exposure_uv,4)                                                                                                 AS exposure_uv_hb_last_value
               ,if(m2.exposure_uv IS NULL OR m2.exposure_uv = 0,NULL,round((m0.exposure_uv - m2.exposure_uv) * 1.0 / m2.exposure_uv,4)) AS exposure_uv_hb_diff_percent
               ,round(m1.exposure_uv,4)                                                                                                 AS exposure_uv_tb_last_value
               ,if(m1.exposure_uv IS NULL OR m1.exposure_uv = 0,NULL,round((m0.exposure_uv - m1.exposure_uv) * 1.0 / m1.exposure_uv,4)) AS exposure_uv_tb_diff_percent
        FROM
        (
                SELECT  m0.pt                          AS pt
                       ,`exposure_uv`                  AS `exposure_uv`
                FROM
                (
                        SELECT  pt                                                                     AS pt
                               ,CASE WHEN COUNT(`exposure_uv`) > 0 THEN AVG(`exposure_uv`)  ELSE 0 END AS `exposure_uv`
                        FROM
                        (
                                SELECT  pt                                         AS pt
                                       ,uniqHLL12Merge(count_distinct_exposure_uv) AS `exposure_uv`
                                FROM test
                                GROUP BY  pt
                        ) m
                        GROUP BY  pt
                ) m0
        ) m0
        LEFT JOIN
        (
                SELECT  m0.pt                          AS pt
                       ,`exposure_uv`                  AS `exposure_uv`
                FROM
                (
                        SELECT  formatDateTime(addYears(parseDateTimeBestEffort(pt),1),'%Y%m%d')       AS pt
                               ,CASE WHEN COUNT(`exposure_uv`) > 0 THEN AVG(`exposure_uv`)  ELSE 0 END AS `exposure_uv`
                        FROM
                        (
                                SELECT  pt                                         AS pt
                                       ,uniqHLL12Merge(count_distinct_exposure_uv) AS `exposure_uv`
                                FROM test
                                GROUP BY  pt
                        ) m
                        GROUP BY  pt
                ) m0
        ) m1
        ON m0.pt = m1.pt
        LEFT JOIN
        (
                SELECT  m0.pt                          AS pt
                       ,`exposure_uv`                  AS `exposure_uv`
                FROM
                (
                        SELECT  formatDateTime(addDays(toDate(parseDateTimeBestEffort(pt)),1),'%Y%m%d') AS pt
                               ,CASE WHEN COUNT(`exposure_uv`) > 0 THEN AVG(`exposure_uv`)  ELSE 0 END  AS `exposure_uv`
                        FROM
                        (
                                SELECT  pt                                         AS pt
                                       ,uniqHLL12Merge(count_distinct_exposure_uv) AS `exposure_uv`
                                FROM test
                                GROUP BY  pt
                        ) m
                        GROUP BY  pt
                ) m0
        ) m2
        ON m0.pt = m2.pt
) c0
ORDER BY pt ASC, exposure_uv DESC
settings join_use_nulls = 1;

CREATE TABLE test1
(
    `pt` String,
    `exposure_uv` Float64
)
ENGINE = Memory;

SELECT  *
FROM
(
        SELECT  m0.pt
               ,m0.exposure_uv AS exposure_uv
               ,round(m2.exposure_uv,4)
        FROM
        (
                SELECT  pt
                       ,exposure_uv
                FROM test1
        ) m0
        LEFT JOIN
        (
                SELECT  pt
                       ,exposure_uv
                FROM test1
        ) m1
        ON m0.pt = m1.pt
        LEFT JOIN
        (
                SELECT  pt
                        ,exposure_uv
                FROM test1
        ) m2
        ON m0.pt = m2.pt
) c0
ORDER BY exposure_uv
settings join_use_nulls = 1;

SELECT
    pt AS pt,
    exposure_uv AS exposure_uv
FROM
(
    SELECT
        pt
    FROM test1
) AS m0
FULL OUTER JOIN
(
    SELECT
        pt,
        exposure_uv
    FROM test1
) AS m1 ON m0.pt = m1.pt;
