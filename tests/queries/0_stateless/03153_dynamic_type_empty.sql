SET allow_experimental_dynamic_type=1;

CREATE TABLE test_null_empty (d Dynamic) ENGINE = Memory;
INSERT INTO test_null_empty VALUES ([]), ([1]), ([]), (['1']), ([]), (()),((1)), (()), (('1')), (()), ({}), ({1:2}), ({}), ({'1':'2'}), ({});
SELECT d, dynamicType(d) FROM test_null_empty;
