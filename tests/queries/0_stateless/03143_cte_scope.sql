-- https://github.com/ClickHouse/ClickHouse/issues/56287
SET enable_analyzer = 1;
DROP TABLE IF EXISTS tmp_a;
DROP TABLE IF EXISTS tmp_b;

CREATE TEMPORARY TABLE IF NOT EXISTS tmp_a
(
    k1 Int32,
    k2 Int32,
    d1 Int32,
    d2 Int32
) ENGINE = Memory;
INSERT INTO tmp_a VALUES (1,2,3,4);
INSERT INTO tmp_a VALUES (5,6,7,8);

CREATE TEMPORARY TABLE IF NOT EXISTS tmp_b (
                                               k1 Int32,
                                               k2 Int32,
                                               d0 Float64
) ENGINE = Memory;
INSERT INTO tmp_b VALUES (1,2,0.3);
INSERT INTO tmp_b VALUES (5,6,0.4);

SELECT tb1.*,tb2.*
FROM
        (
            with tmp0 as (select k1,k2,d1 from tmp_a),
                 tmp_s as (select k1,k2,d0 from tmp_b),
                 tmp1 as (select tmp0.*,tmp_s.d0 from tmp0 left join tmp_s on tmp0.k1=tmp_s.k1 and tmp0.k2=tmp_s.k2)
            select * from tmp1
        ) as tb1
    LEFT JOIN
        (
           with tmp0 as (select k1,k2,d2 from tmp_a),
                 tmp_s as (select k1,k2,d0 from tmp_b),
                 tmp1 as (select tmp0.*,tmp_s.d0 from tmp0 left join tmp_s on tmp0.k1=tmp_s.k1 and tmp0.k2=tmp_s.k2)
            select * from tmp1
        ) as tb2
    ON tb1.k1=tb2.k1 AND tb1.k2=tb2.k2
ORDER BY k1;

DROP TABLE IF EXISTS tmp_a;
DROP TABLE IF EXISTS tmp_b;
