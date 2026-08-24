set allow_suspicious_low_cardinality_types=1;
drop table if exists lc_lambda;
create table lc_lambda (arr Array(LowCardinality(UInt64))) engine = Memory;
insert into lc_lambda select range(number) from system.numbers limit 10;
select arrayFilter(x -> x % 2 == 0, arr) from lc_lambda;
drop table if exists lc_lambda;

drop table if exists test_array;
CREATE TABLE test_array(resources_host Array(LowCardinality(String))) ENGINE = MergeTree() ORDER BY (resources_host);
insert into test_array values (['a']);
SELECT arrayMap(i -> [resources_host[i]], arrayEnumerate(resources_host)) FROM test_array;
drop table if exists test_array;

SELECT arrayMap(x -> (x + (arrayMap(y -> ((x + y) + toLowCardinality(1)), [])[1])), []);
