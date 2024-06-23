create table test1 (
    `pt` String,
    `brand_name` String,
    `total_indirect_order_cnt` Float64,
    `total_indirect_gmv` Float64
) ENGINE = Memory;

create table test2 (
    `pt` String,
    `brand_name` String,
    `exposure_uv` Float64,
    `click_uv` Float64
) ENGINE = Memory;

INSERT INTO test1 (`pt`, `brand_name`, `total_indirect_order_cnt`, `total_indirect_gmv`) VALUES ('20230625', 'LINING', 2232, 1008710), ('20230625', 'adidas', 125, 58820), ('20230625', 'Nike', 1291, 1033020), ('20230626', 'Nike', 1145, 938926), ('20230626', 'LINING', 1904, 853336), ('20230626', 'adidas', 133, 62546), ('20220626', 'LINING', 3747, 1855203), ('20220626', 'Nike', 2295, 1742665), ('20220626', 'adidas', 302, 122388);

INSERT INTO test2 (`pt`, `brand_name`, `exposure_uv`, `click_uv`) VALUES ('20230625', 'Nike', 2012913, 612831),  ('20230625', 'adidas', 480277, 96176), ('20230625', 'LINING', 2474234, 627814), ('20230626', 'Nike', 1934666, 610770), ('20230626', 'adidas', 469904, 91117), ('20230626', 'LINING', 2285142, 599765), ('20220626', 'Nike', 2979656, 937166), ('20220626', 'adidas', 704751, 124250), ('20220626', 'LINING', 3163884, 1010221);

SELECT * FROM (
        SELECT  m0.pt                                                                                     AS pt
               ,m0.`uvctr`                                                                                AS uvctr
               ,round(m1.uvctr,4)                                                                         AS uvctr_hb_last_value
               ,round(m2.uvctr,4)                                                                         AS uvctr_tb_last_value
        FROM
        (
                SELECT  m0.pt                                                                                                           AS pt
                       ,COALESCE(m0.brand_name,m1.brand_name)                                                                           AS brand_name
                       ,if(isNaN(`click_uv` / `exposure_uv`) OR isInfinite(`click_uv` / `exposure_uv`),NULL,`click_uv` / `exposure_uv`) AS `uvctr`
                FROM
                (
                                SELECT  pt          AS pt
                                       ,brand_name  AS `brand_name`
                                       ,exposure_uv AS `exposure_uv`
                                       ,click_uv    AS `click_uv`
                                FROM test2
                                WHERE pt = '20230626'
                ) m0
                FULL JOIN
                (
                                SELECT  pt                        AS pt
                                       ,brand_name                AS `brand_name`
                                       ,total_indirect_order_cnt  AS `total_indirect_order_cnt`
                                       ,total_indirect_gmv        AS `total_indirect_gmv`
                                FROM test1
                                WHERE pt = '20230626'
                ) m1
                ON m0.brand_name = m1.brand_name AND m0.pt = m1.pt
        ) m0
        LEFT JOIN
        (
                SELECT  m0.pt AS pt
                       ,if(isNaN(`click_uv` / `exposure_uv`) OR isInfinite(`click_uv` / `exposure_uv`),NULL,`click_uv` / `exposure_uv`) AS `uvctr`
                       ,COALESCE(m0.brand_name,m1.brand_name)                                                                 AS brand_name
                       ,`exposure_uv`                                                                                         AS `exposure_uv`
                       ,`click_uv`
                FROM
                (
                                SELECT  pt          AS pt
                                       ,brand_name  AS `brand_name`
                                       ,exposure_uv AS `exposure_uv`
                                       ,click_uv    AS `click_uv`
                                FROM test2
                                WHERE pt = '20230625'
                ) m0
                FULL JOIN
                (
                                SELECT  pt                       AS pt
                                       ,brand_name               AS `brand_name`
                                       ,total_indirect_order_cnt AS `total_indirect_order_cnt`
                                       ,total_indirect_gmv       AS `total_indirect_gmv`
                                FROM test1
                                WHERE pt = '20230625'
                ) m1
                ON m0.brand_name = m1.brand_name AND m0.pt = m1.pt
        ) m1
        ON m0.brand_name = m1.brand_name AND m0.pt = m1.pt
        LEFT JOIN
        (
                SELECT  m0.pt AS pt
                       ,if(isNaN(`click_uv` / `exposure_uv`) OR isInfinite(`click_uv` / `exposure_uv`),NULL,`click_uv` / `exposure_uv`) AS `uvctr`
                       ,COALESCE(m0.brand_name,m1.brand_name)                                                                 AS brand_name
                       ,`exposure_uv`                                                                                         AS `exposure_uv`
                       ,`click_uv`
                FROM
                (
                                SELECT  pt          AS pt
                                       ,brand_name  AS `brand_name`
                                       ,exposure_uv AS `exposure_uv`
                                       ,click_uv    AS `click_uv`
                                FROM test2
                                WHERE pt = '20220626'
                ) m0
                FULL JOIN
                (
                                SELECT  pt                        AS pt
                                       ,brand_name                AS `brand_name`
                                       ,total_indirect_order_cnt  AS `total_indirect_order_cnt`
                                       ,total_indirect_gmv        AS `total_indirect_gmv`
                                FROM test1
                                WHERE pt = '20220626'
                ) m1
                ON m0.brand_name = m1.brand_name AND m0.pt = m1.pt
        ) m2
        ON m0.brand_name = m2.brand_name AND m0.pt = m2.pt
) c0
ORDER BY pt ASC, uvctr DESC;

