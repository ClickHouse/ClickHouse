-- Only Float32 supported but can be easily adapted by using non-AVX L2 for non-Float32.
--SELECT arrayL2([5, 5, 5], [5, 5, 5]);
--SELECT arrayL2([1, 2, 3], array(4, 5, 6));
--SELECT arrayL2(array(1.123, 2.456, 3.789), [4.789, 5.456, 6.123]);
--0
--27
--27.887112

CREATE TABLE t (`a` Array(Float32), `b` Array(Float32)) ENGINE = MergeTree ORDER BY tuple(a, b);

INSERT INTO t VALUES
    ([5, 5, 5], array(5, 5, 5)), (array(4, 5, 6), [1, 2, 3]), ([1.123, 2.456, 3.789], array(4.789, 5.456, 6.123))

select arrayL2(a, b) from t
