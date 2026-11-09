DROP TABLE IF EXISTS test;

set allow_experimental_nullable_tuple_type=1;

CREATE TABLE test (a Nullable(Tuple(x Array(UInt16))));
INSERT INTO test SELECT tuple(range(number % 1000)) FROM numbers_mt(1000);

SELECT DISTINCT count() IGNORE NULLS FROM test PREWHERE notEmpty(a.x) WHERE notEmpty(a.x);

DROP TABLE test;
