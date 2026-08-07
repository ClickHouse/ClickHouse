SELECT 'Basic tests';
SELECT toBFloat16(-1) IN [0, 1, 2] AS result;
SELECT toBFloat16(-1) IN [-2, -1, 0, 1, 2] AS result;
SELECT toBFloat16(-1) IN [toFloat32(-2), toFloat32(-1), toFloat32(0), toFloat32(1), toFloat32(2)] AS result;
SELECT toBFloat16(-1) IN [toFloat64(-2), toFloat64(-1), toFloat64(0), toFloat64(1), toFloat64(2)] AS result;
SELECT toFloat64(-1) IN [toBFloat16(-2), toBFloat16(-1), toBFloat16(0), toBFloat16(1), toBFloat16(2)] AS result;

SELECT 'Edge cases';
SELECT toBFloat16(0) IN [-0] AS result;
SELECT toBFloat16(0/0) IN [0/0] AS result;
SELECT toBFloat16(0/0) IN [-0/0] AS result;
SELECT toBFloat16(1/0) IN [1/0] AS result;
SELECT toBFloat16(1/0) IN [-1/0] AS result;

SELECT 'Compare to Float32';
SELECT toFloat32(0) IN [-0] AS result;
SELECT toFloat32(0/0) IN [0/0] AS result;
SELECT toFloat32(0/0) IN [-0/0] AS result;
SELECT toFloat32(1/0) IN [1/0] AS result;
SELECT toFloat32(1/0) IN [-1/0] AS result;
