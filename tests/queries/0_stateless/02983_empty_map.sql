--https://github.com/ClickHouse/ClickHouse/issues/59402
CREATE TABLE t1
(
    f1 Int32,
    f2 Map(LowCardinality(String),LowCardinality(String)),
    f3 Map(String,String),
    f4 Map(Int32,Int32)
)
ENGINE=Memory AS
SELECT 1 as f1,
       map(number%2,number%10) as f2,
       f2 as f3,
       f2 as f4
from numbers(1000111);

SET max_block_size=10;

-- { echoOn }
SELECT f1, f2['2'], count() FROM t1 GROUP BY 1,2 order by 1,2;
SELECT f1, f3['2'], count() FROM t1 GROUP BY 1,2 order by 1,2;
SELECT f1, f4[2], count() FROM t1 GROUP BY 1,2 order by 1,2;
