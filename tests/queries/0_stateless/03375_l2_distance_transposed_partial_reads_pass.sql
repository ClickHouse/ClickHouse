-- https://github.com/ClickHouse/ClickHouse/issues/88362

SET allow_experimental_qbit_type = 1;

CREATE TABLE qbit (id UInt32, vec QBit(BFloat16, 1)) ENGINE = Memory;
INSERT INTO qbit VALUES (1, [toBFloat16(1)]);

WITH [toBFloat16(2)] AS reference_vec SELECT id, round(L2DistanceTransposed(vec, reference_vec, toNullable(1)), 5) AS dist FROM qbit;
WITH [toBFloat16(2)] AS reference_vec SELECT id, round(L2DistanceTransposed(vec, reference_vec, toLowCardinality(toNullable(1))), 5) AS dist FROM qbit;
DROP TABLE qbit;
