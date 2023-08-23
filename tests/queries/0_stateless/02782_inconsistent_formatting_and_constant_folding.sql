SELECT -0, toTypeName(-0), -1, toTypeName(-1), -0., toTypeName(-0.);

DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t7;

create table t4 (c26 String) engine = Log;
create view t7 as select max(ref_3.c26) as c_2_c46_1 from t4 as ref_3;

select
        c_7_c4585_14 as c_4_c4593_5
      from
        (select
              avg(0) as c_7_c4572_1,
              max(-0) as c_7_c4585_14
            from
              t7 as ref_0
            group by ref_0.c_2_c46_1) as subq_0
where c_4_c4593_5 <= multiIf(true, 1, exp10(c_4_c4593_5) <= 1, 1, 1);

select x as c
      from
        (select 1 AS k,
              max(0) as a,
              max(-0) as x
            from
              t7 GROUP BY k)
where NOT ignore(c);

SELECT x
FROM
(
    SELECT
        avg(0) AS c_7_c4572_1,
        max(-0) AS x
    FROM t7 AS ref_0
    GROUP BY ref_0.c_2_c46_1
)
WHERE x <= multiIf(true, 1, exp10(x) <= 1, 1, 1);

DROP TABLE t7;
DROP TABLE t4;
