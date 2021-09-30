DROP TABLE IF EXISTS test1;

CREATE TABLE test1(i int, j int) ENGINE Log;

INSERT INTO test1 VALUES (1, 2), (3, 4);

WITH test1 AS (SELECT * FROM numbers(5)) SELECT * FROM test1;
WITH test1 AS (SELECT i + 1, j + 1 FROM test1) SELECT * FROM test1;
WITH test1 AS (SELECT i + 1, j + 1 FROM test1) SELECT * FROM (SELECT * FROM test1);
SELECT * FROM (WITH test1 AS (SELECT toInt32(*) i FROM numbers(5)) SELECT * FROM test1) l ANY INNER JOIN test1 r on (l.i == r.i);
WITH test1 AS (SELECT i + 1, j + 1 FROM test1) SELECT toInt64(4) i, toInt64(5) j FROM numbers(3) WHERE (i, j) IN test1;

DROP TABLE IF EXISTS test1;

select '---------------------------';

set empty_result_for_aggregation_by_empty_set = 0;

WITH test1 AS (SELECT number-1 as n FROM numbers(42)) 
SELECT max(n+1)+1 z FROM test1;

WITH test1 AS (SELECT number-1 as n FROM numbers(42)) 
SELECT max(n+1)+1 z FROM test1 join test1 x using n having z - 1 = (select min(n-1)+41 from test1) + 2;

WITH test1 AS (SELECT number-1 as n FROM numbers(4442) order by n limit 100)
SELECT max(n) FROM test1 where n=422;

WITH test1 AS (SELECT number-1 as n FROM numbers(4442) order by n limit 100)
SELECT max(n) FROM test1 where n=42;

drop table if exists with_test ;
create table with_test engine=Memory as select cast(number-1 as Nullable(Int64))  n from numbers(10000);

WITH test1 AS (SELECT n FROM with_test where n <= 40) 
SELECT max(n+1)+1 z FROM test1 join test1 x using (n) having max(n+1)+1 - 1 = (select min(n-1)+41 from test1) + 2;

WITH test1 AS (SELECT n FROM with_test where n <= 40) 
SELECT max(n+1)+1 z FROM test1 join test1 x using (n) having z - 1 = (select min(n-1)+41 from test1) + 2;

WITH test1 AS (SELECT  n FROM with_test order by n limit 100)
SELECT max(n) FROM test1 where n=422;

WITH test1 AS (SELECT n FROM with_test order by n limit 100)
SELECT max(n) FROM test1 where n=42;

WITH test1 AS (SELECT n FROM with_test where n = 42  order by n limit 100)
SELECT max(n) FROM test1 where n=42;

WITH test1 AS (SELECT n FROM with_test where n = 42 or 1=1 order by n limit 100)
SELECT max(n) FROM test1 where n=42;

WITH test1 AS (SELECT n, null as b FROM with_test where n = 42 or b is null order by n limit 100)
SELECT max(n) FROM test1 where n=42;

WITH test1 AS (SELECT n, null b FROM with_test where b is null)
SELECT max(n) FROM test1 where n=42;

WITH test1 AS (SELECT n, null b FROM with_test where b is null or 1=1)
SELECT max(n) FROM test1 where n=45;

WITH test1 AS (SELECT n, null b FROM with_test where b is null and n = 42)
SELECT max(n) FROM test1 where n=45;

WITH test1 AS (SELECT n, null b FROM with_test where 1=1 and n = 42 order by n)
SELECT max(n) FROM test1 where n=45;

WITH test1 AS (SELECT n, null b, n+1 m FROM with_test where 1=0 or n = 42 order by n limit 4)
SELECT max(n) m FROM test1 where test1.m=43 having max(n)=42;

WITH test1 AS (SELECT n, null b, n+1 m FROM with_test where  n = 42 order by n limit 4)
SELECT max(n) m FROM test1 where b is null and test1.m=43 having m=42 limit 4;

with
    test1 as (select n, null b, n+1 m from with_test where  n = 42 order by n limit 4),
    test2 as (select n + 1 as x, n - 1 as y from test1),
    test3 as (select x * y as z from test2)
select z + 1 as q from test3;

drop table  with_test ;
