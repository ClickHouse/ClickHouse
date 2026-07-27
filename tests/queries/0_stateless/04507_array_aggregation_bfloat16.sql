-- Aggregation functions over arrays of BFloat16.

SELECT arraySum([toBFloat16(1), toBFloat16(2), toBFloat16(3), toBFloat16(4)]);
SELECT arraySum([toBFloat16(1.5), toBFloat16(2.5), toBFloat16(3)]) AS s, toTypeName(s);
SELECT arraySum(x -> x * 2, [toBFloat16(1), toBFloat16(2)]);
SELECT arrayAvg([toBFloat16(1), toBFloat16(2), toBFloat16(3), toBFloat16(4)]);
SELECT arrayProduct([toBFloat16(2), toBFloat16(3), toBFloat16(4)]);
SELECT arrayMin([toBFloat16(5), toBFloat16(2), toBFloat16(7)]), arrayMax([toBFloat16(5), toBFloat16(2), toBFloat16(7)]);

-- Empty arrays.
SELECT arraySum([]::Array(BFloat16)), arrayProduct([]::Array(BFloat16));
SELECT arrayAvg([]::Array(BFloat16));

-- Empty arrays with a constant lambda: the result must be the default value, same as for other numeric types.
SELECT arraySum(x -> toBFloat16(1), []::Array(BFloat16)), arrayAvg(x -> toBFloat16(1), []::Array(BFloat16)), arrayProduct(x -> toBFloat16(2), []::Array(BFloat16));

-- A column of arrays.
DROP TABLE IF EXISTS t_array_bfloat16;
CREATE TABLE t_array_bfloat16 (arr Array(BFloat16)) ENGINE = Memory;
INSERT INTO t_array_bfloat16 VALUES ([1, 2, 3]), ([]), ([10, 20]);
SELECT arraySum(arr) AS s, toTypeName(s) FROM t_array_bfloat16 ORDER BY s;
DROP TABLE t_array_bfloat16;
