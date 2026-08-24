-- Tags: no-fasttest

SELECT 'uniqTheta union test';

select finalizeAggregation(uniqThetaUnion(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select arrayReduce('uniqThetaState',[]) as a, arrayReduce('uniqThetaState',[]) as b );

select finalizeAggregation(uniqThetaUnion(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select arrayReduce('uniqThetaState',[1,2]) as a, arrayReduce('uniqThetaState',[2,3,4]) as b );

select finalizeAggregation(uniqThetaUnion(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select arrayReduce('uniqThetaState',[2,3,4]) as a, arrayReduce('uniqThetaState',[1,2]) as b );

SELECT 'uniqTheta intersect test';

select finalizeAggregation(uniqThetaIntersect(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select arrayReduce('uniqThetaState',[]) as a, arrayReduce('uniqThetaState',[]) as b );

select finalizeAggregation(uniqThetaIntersect(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select arrayReduce('uniqThetaState',[1,2]) as a, arrayReduce('uniqThetaState',[2,3,4]) as b );

select finalizeAggregation(uniqThetaIntersect(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select arrayReduce('uniqThetaState',[2,3,4]) as a, arrayReduce('uniqThetaState',[1,2]) as b );

SELECT 'uniqTheta union test';

select finalizeAggregation(uniqThetaNot(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select arrayReduce('uniqThetaState',[]) as a, arrayReduce('uniqThetaState',[]) as b );

select finalizeAggregation(uniqThetaNot(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select arrayReduce('uniqThetaState',[1,2]) as a, arrayReduce('uniqThetaState',[2,3,4]) as b );

select finalizeAggregation(uniqThetaNot(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select arrayReduce('uniqThetaState',[2,3,4]) as a, arrayReduce('uniqThetaState',[1,2]) as b );

SELECT 'uniqTheta retention test';

select finalizeAggregation(uniqThetaIntersect(a,b)), finalizeAggregation(a),finalizeAggregation(b) from 
(
select (uniqThetaStateIf(number, number>0)) as a, (uniqThetaStateIf(number, number>5)) as b 
from 
(select  number  FROM system.numbers LIMIT 10)
);

SELECT 'uniqTheta retention with AggregatingMergeTree test';
DROP TABLE IF EXISTS test1;

CREATE TABLE test1
(
    `year` String ,
    `uv` AggregateFunction(uniqTheta, Int64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (year);

INSERT INTO TABLE test1(year, uv) select '2021',uniqThetaState(toInt64(1));
INSERT INTO TABLE test1(year, uv) select '2021',uniqThetaState(toInt64(2));
INSERT INTO TABLE test1(year, uv) select '2021',uniqThetaState(toInt64(3));
INSERT INTO TABLE test1(year, uv) select '2021',uniqThetaState(toInt64(4));
INSERT INTO TABLE test1(year, uv) select '2022',uniqThetaState(toInt64(1));
INSERT INTO TABLE test1(year, uv) select '2022',uniqThetaState(toInt64(3));

select finalizeAggregation(uniqThetaIntersect(uv2021,uv2022))/finalizeAggregation(uv2021),finalizeAggregation(uniqThetaIntersect(uv2021,uv2022)),finalizeAggregation(uv2021)
from
(
select uniqThetaMergeStateIf(uv,year='2021') as uv2021, uniqThetaMergeStateIf(uv,year='2022') as uv2022 
from test1
);

DROP TABLE IF EXISTS test1;

SELECT 'uniqTheta retention with MergeTree test';
DROP TABLE IF EXISTS test2;

CREATE TABLE test2
(
    `year` String ,
    `uv`  Int64
)
ENGINE = MergeTree()
ORDER BY (year);

INSERT INTO TABLE test2(year, uv) select '2021',1;
INSERT INTO TABLE test2(year, uv) select '2021',2;
INSERT INTO TABLE test2(year, uv) select '2021',3;
INSERT INTO TABLE test2(year, uv) select '2021',4;
INSERT INTO TABLE test2(year, uv) select '2022',1;
INSERT INTO TABLE test2(year, uv) select '2022',3;

select finalizeAggregation(uniqThetaIntersect(uv2021,uv2022))/finalizeAggregation(uv2021),finalizeAggregation(uniqThetaIntersect(uv2021,uv2022)),finalizeAggregation(uv2021)
from
(
select uniqThetaStateIf(uv,year='2021') as uv2021, uniqThetaStateIf(uv,year='2022') as uv2022 
from test2
);



DROP TABLE IF EXISTS test2;
