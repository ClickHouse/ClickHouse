-- NEAREST JOIN: for every left row, pick the right row with the minimal vector distance
-- among the rows that share the equality keys.

DROP TABLE IF EXISTS upload_addresses;
DROP TABLE IF EXISTS base_addresses;

CREATE TABLE base_addresses
(
    zip String,
    house_number UInt32,
    street_embedding Array(Float32),
    address String
) ENGINE = MergeTree ORDER BY (zip, house_number);

INSERT INTO base_addresses VALUES
    ('10004', 123, [1.0, 0.0, 0.2], '123 N Main St'),
    ('10004', 123, [0.0, 1.0, 0.4], '123 Maple Ave'),
    ('10004', 456, [1.0, 0.1, 0.0], '456 N Main St'),
    ('94103', 123, [0.9, 0.0, 0.1], '123 W Main St'),
    ('94103', 789, [0.2, 0.8, 0.0], '789 Oak Blvd');

CREATE TABLE upload_addresses
(
    query_id UInt32,
    zip String,
    house_number UInt32,
    street_embedding Array(Float32)
) ENGINE = MergeTree ORDER BY query_id;

INSERT INTO upload_addresses VALUES
    (1, '10004', 123, [0.95, 0.05, 0.15]),
    (2, '10004', 123, [0.05, 0.9, 0.35]),
    (3, '94103', 123, [1.0, 0.0, 0.0]),
    (4, '94103', 999, [1.0, 0.0, 0.0]);

SELECT 'inner nearest join, L2Distance';
SELECT upload.query_id, base.address
FROM upload_addresses AS upload
NEAREST JOIN base_addresses AS base
    ON upload.zip = base.zip AND upload.house_number = base.house_number
        AND L2Distance(upload.street_embedding, base.street_embedding)
ORDER BY upload.query_id;

SELECT 'left nearest join, L2Distance';
SELECT upload.query_id, base.address
FROM upload_addresses AS upload
NEAREST LEFT JOIN base_addresses AS base
    ON upload.zip = base.zip AND upload.house_number = base.house_number
        AND L2Distance(upload.street_embedding, base.street_embedding)
ORDER BY upload.query_id;

SELECT 'inner nearest join, cosineDistance';
SELECT upload.query_id, base.address
FROM upload_addresses AS upload
NEAREST JOIN base_addresses AS base
    ON upload.zip = base.zip AND upload.house_number = base.house_number
        AND cosineDistance(upload.street_embedding, base.street_embedding)
ORDER BY upload.query_id;

SELECT 'distance function arguments in reverse order';
SELECT upload.query_id, base.address
FROM upload_addresses AS upload
NEAREST JOIN base_addresses AS base
    ON upload.zip = base.zip AND upload.house_number = base.house_number
        AND L2Distance(base.street_embedding, upload.street_embedding)
ORDER BY upload.query_id;

SELECT 'strictness after the kind keyword';
SELECT upload.query_id, base.address
FROM upload_addresses AS upload
INNER NEAREST JOIN base_addresses AS base
    ON upload.zip = base.zip AND upload.house_number = base.house_number
        AND L2Distance(upload.street_embedding, base.street_embedding)
ORDER BY upload.query_id;

SELECT 'the distance is recomputable from the output columns';
SELECT upload.query_id, base.address, round(L2Distance(upload.street_embedding, base.street_embedding), 4) AS distance
FROM upload_addresses AS upload
NEAREST JOIN base_addresses AS base
    ON upload.zip = base.zip AND upload.house_number = base.house_number
        AND L2Distance(upload.street_embedding, base.street_embedding)
ORDER BY upload.query_id;

SELECT 'a single equality key';
SELECT upload.query_id, base.address
FROM upload_addresses AS upload
NEAREST JOIN base_addresses AS base
    ON upload.zip = base.zip
        AND L2Distance(upload.street_embedding, base.street_embedding)
ORDER BY upload.query_id;

SELECT 'Float32 and Float64 vectors are joined through the common supertype';
SELECT upload.query_id, base.address
FROM upload_addresses AS upload
NEAREST JOIN
(
    SELECT zip, house_number, CAST(street_embedding, 'Array(Float64)') AS street_embedding, address
    FROM base_addresses
) AS base
    ON upload.zip = base.zip AND upload.house_number = base.house_number
        AND L2Distance(upload.street_embedding, base.street_embedding)
ORDER BY upload.query_id;

SELECT 'a single-side condition in ON excludes candidates before picking the nearest';
SELECT upload.query_id, base.address
FROM upload_addresses AS upload
NEAREST JOIN base_addresses AS base
    ON upload.zip = base.zip AND upload.house_number = base.house_number
        AND L2Distance(upload.street_embedding, base.street_embedding)
        AND base.address != '123 N Main St'
ORDER BY upload.query_id;

SELECT 'LEFT with join_use_nulls: unmatched right columns are NULL';
SELECT upload.query_id, base.address
FROM upload_addresses AS upload
NEAREST LEFT JOIN base_addresses AS base
    ON upload.zip = base.zip AND upload.house_number = base.house_number
        AND L2Distance(upload.street_embedding, base.street_embedding)
ORDER BY upload.query_id
SETTINGS join_use_nulls = 1;

SELECT 'formatted query keeps the NEAREST keyword';
SELECT formatQuerySingleLine('SELECT 1 FROM t1 NEAREST LEFT JOIN t2 ON t1.k = t2.k AND L2Distance(t1.v, t2.v)');

DROP TABLE upload_addresses;
DROP TABLE base_addresses;
