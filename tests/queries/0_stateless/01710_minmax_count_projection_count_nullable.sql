DROP TABLE IF EXISTS test;

CREATE TABLE test (`val` LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;

insert into test select number == 3 ? 'some value' : null from numbers(5);

SELECT count(val) FROM test SETTINGS optimize_use_implicit_projections = 1;

DROP TABLE test;
