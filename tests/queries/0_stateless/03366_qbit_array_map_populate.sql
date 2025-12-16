SET allow_experimental_qbit_type = 1;

SELECT 'Test QBit population with arrayMap: Float64';

DROP TABLE IF EXISTS qbits;
CREATE TABLE qbits (id UInt32, vec QBit(Float64, 9)) ENGINE = Memory;
INSERT INTO qbits SELECT number + 1 AS id, arrayMap(i -> toFloat64(i + number), range(9)) AS vec FROM numbers(9);
SELECT * FROM qbits ORDER BY id;
DROP TABLE qbits;


SELECT 'Test QBit population with arrayMap: Float32';

CREATE TABLE qbits (id UInt32, vec QBit(Float32, 9)) ENGINE = Memory;
INSERT INTO qbits SELECT number + 1 AS id, arrayMap(i -> toFloat32(i + number), range(9)) AS vec FROM numbers(9);
SELECT * FROM qbits ORDER BY id;
DROP TABLE qbits;


SELECT 'Test QBit population with arrayMap: BFloat16';

CREATE TABLE qbits (id UInt32, vec QBit(BFloat16, 9)) ENGINE = Memory;
INSERT INTO qbits SELECT number + 1 AS id, arrayMap(i -> toBFloat16(i + number), range(9)) AS vec FROM numbers(9);
SELECT * FROM qbits ORDER BY id;
DROP TABLE qbits;
