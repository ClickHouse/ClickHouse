select sumMerge(y) from 
(
  select cast(x, 'AggregateFunction(sum, Decimal(50, 10))') y from 
   (
     select arrayReduce('sumState', [toDecimal256('0.000001', 10), toDecimal256('1.1', 10)]) x
   )
);

select minMerge(y) from 
(
  select cast(x, 'AggregateFunction(min, Decimal(18, 10))') y from 
   (
     select arrayReduce('minState', [toDecimal64('0.000001', 10), toDecimal64('1.1', 10)]) x
   )
);


drop table if exists consumer_02366;
drop table if exists producer_02366;
drop table if exists mv_02366;

CREATE TABLE consumer_02366
(
    `id` UInt16,
    `dec` AggregateFunction(argMin, Decimal(24, 10), UInt16)
)
ENGINE = AggregatingMergeTree
PRIMARY KEY id
ORDER BY id;

CREATE TABLE producer_02366
(
    `id` UInt16,
    `dec` String
)
ENGINE = MergeTree
PRIMARY KEY id
ORDER BY id;

CREATE MATERIALIZED VIEW mv_02366 TO consumer_02366 AS
SELECT
    id,
    argMinState(dec, id) AS dec
FROM
(
    SELECT
        id,
        toDecimal128(dec, 10) AS dec
    FROM producer_02366
)
GROUP BY id;

INSERT INTO producer_02366 (*) VALUES (19, '.1');

SELECT
    id,
    finalizeAggregation(dec)
FROM consumer_02366;

drop table consumer_02366;
drop table producer_02366;
drop table mv_02366;
