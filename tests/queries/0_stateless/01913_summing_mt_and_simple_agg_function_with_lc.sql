drop table if exists smta;

CREATE TABLE smta
(
    `k` Int64,
    `a` AggregateFunction(max, Int64),
    `city` SimpleAggregateFunction(max, LowCardinality(String))
)
ENGINE = SummingMergeTree
ORDER BY k;

insert into smta(k, city) values (1, 'x');

select k, city from smta;

insert into smta(k, city) values (1, 'y');
optimize table smta;

select k, city from smta;

drop table if exists smta;
