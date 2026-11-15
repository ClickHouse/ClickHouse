set allow_experimental_variant_type = 1;
set allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS test;

CREATE TABLE test(
    key String,
    val Map(String, Variant(String, Int32, DateTime64(3, 'UTC')))
) engine = ReplicatedMergeTree('/clickhouse/tables/{database}/table', '1')
order by key;

insert into test VALUES ('a', {'a':'a', 'b':1, 'c': '2020-01-01 10:10:10.11'});
insert into test VALUES ('', {'':'xx', '':4});
insert into test VALUES ('', {'x':'xx'});
insert into test VALUES ('', {});
insert into test VALUES ('a', {'a':'a', 'b':1, 'c': '2020-01-01 10:10:10'});
insert into test VALUES ('a', {'a':'b', 'b':1, 'c': '2020-01-01'});
insert into test VALUES ('z', {'a':'a'});
insert into test VALUES ('a', {'a': Null});
insert into test VALUES ('a', {'a': Null, 'a': Null});
insert into test VALUES ('a', {'a': Null, 'c': Null});

SELECT variantElement(arrayJoin(mapValues(val)), 'String') FROM test ORDER BY ALL;
select '---';
SELECT key, arrayJoin(mapValues(val)) FROM test ORDER BY ALL;

DROP TABLE test;
