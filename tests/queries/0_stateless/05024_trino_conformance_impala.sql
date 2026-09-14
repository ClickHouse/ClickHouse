-- Conformance queries derived from the functional tests of Apache Impala
-- (https://github.com/apache/impala, Apache License 2.0) - the subset of
-- generic SQL expressions that is also valid Trino SQL.
-- Expected results verified against the .test files of the original tests.

SET allow_experimental_trino_dialect = 1;
SET dialect = 'trino';


SELECT '-- exprs.test';
select 1+2;
select NULL in (1, 2, 3);
select NULL in (1, NULL, 3);
select 1 in (2, NULL, 1);
select 1 in (1, NULL, 2);
select 1 in (2, 3, 4);
select NULL not in (1, 2, 3);
select NULL not in (1, NULL, 3);
select 1 not in (2, NULL, 1);
select 1 not in (1, NULL, 2);
select 1 not in (2, 3, 4);
select NULL in ('a', NULL, 'b');
select NULL not in ('a', NULL, 'b');
select NULL not in (1.0, NULL, 2.0);
select NULL in (1.0, NULL, 2.0);
select NULL in (true, NULL, false);
select NULL not in (true, NULL, false);
select round(cast(1.1 as float), 2), round(cast(1.2 as float), 4), round(cast(1.111 as double), 2);
select 1.1 * 1.1 + cast(1.1 as float);
select 1.1 * 1.1 + cast(1.1 as decimal(2,1));
select 1.1 * 1.1 + 1.1;
select abs(cast(1 as int)), abs(cast(1 as smallint)), abs(cast(1 as tinyint)), abs(cast(8589934592 as bigint)), abs(cast(-1.3 as double)), abs(cast(-1.3 as float)), abs(cast(-1.32223 as decimal(8,3)));
select NULL <=> NULL;
select NULL <=> 1;
select NULL IS DISTINCT FROM NULL;
select NULL IS DISTINCT FROM 3.14;
select cast(0 as bigint) IS DISTINCT FROM NULL;
select 2.78 IS DISTINCT FROM 3.14;
select 2.78 IS NOT DISTINCT FROM 3.14;
select cast('1.23' as double), cast('.1.23' as float), cast('123.456.' as double), cast('1.23.456' as double), cast('1.23.4.5' as float), cast('0..e' as double);
select 'esca' not like 'esc\_';
select 'escape' like 'esc%';
select 'escape' like 'escap_';
select 'escape' like '%esc%';
select 'escape' like '%escap_';
select greatest(cast(18.3 as decimal(3,1)), cast(19.44 as decimal(4,2))), least(cast(18.3 as decimal(3,1)), cast(19.44 as decimal(4,2))), greatest(cast(19.44 as decimal(4,2)), cast(18.3 as decimal(3,1))), least(cast(19.44 as decimal(4,2)), cast(18.3 as decimal(3,1)));

SELECT '-- values.test';
values(1, 2+1, 1.0, 5.0 + 1.0, 'a');
values(1+1, 2, 5.0, 'a') order by 1 limit 10;
select cast(1 - 0.56850423426112684 as double), cast(1 - 0.768504234261127 as double), cast(0.56850423426112684 -1 as double), cast(0.768504234261127 -1 as double), cast(1 - 0.56850423426112684432 as double);

SELECT '-- date.test';
select cast('2001-1-21' as date), cast('2001-1-2' as date);
select date '2001-1-21', date '2001-1-2';
select cast('2025-08-31 06:23:24.1234567890123456789' as DATE);

SELECT '-- union.test';
select * from (select 1 a, 2 b union all select 3, 4 union all select 10, 20) t where a > b;
-- The original test nests 29 unions; the nesting is reduced here because the deeper
-- form exhausts the stack in the debug and sanitizer builds.
SELECT t1.dt FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT '20210101' AS dt UNION ALL SELECT '20210102' AS dt) AS t UNION ALL SELECT '20210103' AS dt) AS t UNION ALL SELECT '20210104' AS dt) AS t UNION ALL SELECT '20210105' AS dt) AS t UNION ALL SELECT '20210106' AS dt) AS t UNION ALL SELECT '20210107' AS dt) AS t UNION ALL SELECT '20210108' AS dt) AS t UNION ALL SELECT '20210109' AS dt) AS t UNION ALL SELECT '20210110' AS dt) AS t) AS t1 CROSS JOIN (SELECT 1000+1000) AS t2 GROUP BY t1.dt ORDER BY t1.dt;
