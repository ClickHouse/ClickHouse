select xxHash32(NULL);
select xxHash64(NULL);

DROP TABLE IF EXISTS test_hash_on_null;
CREATE TABLE test_hash_on_null (a Array(Nullable(Int64))) ENGINE = Memory;
insert into test_hash_on_null values (NULL) ([NULL, NULL])
select xxHash32(a) from test_hash_on_null;

SELECT cityHash64([1])
SELECT cityHash64([toNullable(1)])
