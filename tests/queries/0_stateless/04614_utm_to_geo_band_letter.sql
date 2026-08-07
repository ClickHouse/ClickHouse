-- Tests that UTMToGeo accepts the MGRS latitude band letter returned by geoToUTM as its fourth argument,
-- deriving the hemisphere from the band ('C'..'M' southern, 'N'..'X' northern).
-- https://github.com/ClickHouse/ClickHouse/issues/111510

SELECT '-- round trip through the band letter matches the input (within 1e-6 degrees)';
WITH (2.294497, 48.858222) AS p, geoToUTM(p.1, p.2) AS u, UTMToGeo(u.1, u.2, u.3, u.4) AS g
    SELECT abs(g.1 - p.1) < 1e-6 AND abs(g.2 - p.2) < 1e-6;   -- Eiffel Tower, northern (31U)
WITH (151.2093, -33.8688) AS p, geoToUTM(p.1, p.2) AS u, UTMToGeo(u.1, u.2, u.3, u.4) AS g
    SELECT abs(g.1 - p.1) < 1e-6 AND abs(g.2 - p.2) < 1e-6;   -- Sydney, southern (56H)

SELECT '-- the band letter and the equivalent is_north flag give the same result';
WITH geoToUTM(2.294497, 48.858222) AS u
    SELECT UTMToGeo(u.1, u.2, u.3, u.4) = UTMToGeo(u.1, u.2, u.3, u.4 >= 'N');   -- northern
WITH geoToUTM(151.2093, -33.8688) AS u
    SELECT UTMToGeo(u.1, u.2, u.3, u.4) = UTMToGeo(u.1, u.2, u.3, u.4 >= 'N');   -- southern

SELECT '-- the hemisphere boundary: band N is northern, band M is southern';
WITH (3.0, 4.0) AS p, geoToUTM(p.1, p.2) AS u, UTMToGeo(u.1, u.2, u.3, u.4) AS g
    SELECT u.4, abs(g.1 - p.1) < 1e-6 AND abs(g.2 - p.2) < 1e-6;   -- band N, northern
WITH (3.0, -4.0) AS p, geoToUTM(p.1, p.2) AS u, UTMToGeo(u.1, u.2, u.3, u.4) AS g
    SELECT u.4, abs(g.1 - p.1) < 1e-6 AND abs(g.2 - p.2) < 1e-6;   -- band M, southern

SELECT '-- the band letter can be given as a String literal and is case-insensitive';
SELECT UTMToGeo(448251.6, 5411935.13, 31, 'U') = UTMToGeo(448251.6, 5411935.13, 31, 1);
SELECT UTMToGeo(448251.6, 5411935.13, 31, 'u') = UTMToGeo(448251.6, 5411935.13, 31, 1);
SELECT UTMToGeo(334368.634, 6250948.345, 56, 'H') = UTMToGeo(334368.634, 6250948.345, 56, 0);

SELECT '-- vectorized over a table with band letters';
SELECT abs(g.1 - lon) < 1e-6 AND abs(g.2 - lat) < 1e-6
FROM (
    SELECT lon, lat, geoToUTM(lon, lat) AS u, UTMToGeo(u.1, u.2, u.3, u.4) AS g
    FROM values('lon Float64, lat Float64', (2.294497, 48.858222), (151.2093, -33.8688), (55.2744, 25.1972))
) ORDER BY lon;

SELECT '-- error handling: the band letter must be a single letter in C..X excluding I and O';
SELECT UTMToGeo(500000., 0., 31, 'I'); -- { serverError BAD_ARGUMENTS } -- 'I' is skipped in the MGRS band sequence
SELECT UTMToGeo(500000., 0., 31, 'O'); -- { serverError BAD_ARGUMENTS } -- 'O' is skipped
SELECT UTMToGeo(500000., 0., 31, 'A'); -- { serverError BAD_ARGUMENTS } -- below the C..X range
SELECT UTMToGeo(500000., 0., 31, 'B'); -- { serverError BAD_ARGUMENTS }
SELECT UTMToGeo(500000., 0., 31, 'Y'); -- { serverError BAD_ARGUMENTS } -- above the C..X range
SELECT UTMToGeo(500000., 0., 31, 'Z'); -- { serverError BAD_ARGUMENTS }
SELECT UTMToGeo(500000., 0., 31, ''); -- { serverError BAD_ARGUMENTS } -- empty
SELECT UTMToGeo(500000., 0., 31, 'NX'); -- { serverError BAD_ARGUMENTS } -- more than one letter

SELECT '-- the integer flag still works and still rejects values other than 0 or 1';
SELECT UTMToGeo(448251.6, 5411935.13, 31, 1).1 > 0;
SELECT UTMToGeo(448251.6, 5411935.13, 31, 2); -- { serverError BAD_ARGUMENTS }

SELECT '-- NULL rows propagate to NULL instead of failing the band validation';
SELECT isNull(g), g = UTMToGeo(448251.6, 5411935.13, 31, 1)
FROM (SELECT UTMToGeo(448251.6, 5411935.13, 31, band) AS g FROM values('band Nullable(String)', (NULL), ('U')));
SELECT isNull(g), g = UTMToGeo(334368.634, 6250948.345, 56, 0)
FROM (SELECT UTMToGeo(334368.634, 6250948.345, 56, band) AS g FROM values('band Nullable(FixedString(1))', (NULL), ('H')));
SELECT isNull(g), g = UTMToGeo(448251.6, 5411935.13, 31, 1)
FROM (SELECT UTMToGeo(448251.6, 5411935.13, 31, band) AS g FROM values('band LowCardinality(Nullable(String))', (NULL), ('U')));
SELECT toTypeName(UTMToGeo(448251.6, 5411935.13, 31, band)) FROM values('band Nullable(String)', (NULL)) LIMIT 1;

SELECT '-- NULL propagation works for the other arguments and for the integer flag too';
SELECT isNull(UTMToGeo(easting, 5411935.13, 31, 'U')) FROM values('easting Nullable(Float64)', (NULL), (448251.6));
SELECT isNull(UTMToGeo(448251.6, 5411935.13, zone, 'U')) FROM values('zone Nullable(UInt8)', (NULL), (31));
SELECT isNull(UTMToGeo(448251.6, 5411935.13, 31, flag)) FROM values('flag Nullable(UInt8)', (NULL), (1));
SELECT UTMToGeo(448251.6, 5411935.13, 31, NULL);

SELECT '-- non-NULL rows of a Nullable band column are still validated';
SELECT UTMToGeo(500000., 0., 31, band) FROM values('band Nullable(String)', ('N'), ('?')); -- { serverError BAD_ARGUMENTS }

SELECT '-- Variant and Dynamic band arguments still dispatch through the default adaptors';
SELECT UTMToGeo(448251.6, 5411935.13, 31, CAST('U', 'Variant(UInt8, String)')) = UTMToGeo(448251.6, 5411935.13, 31, 1);
SELECT UTMToGeo(334368.634, 6250948.345, 56, CAST(0, 'Variant(UInt8, String)')) = UTMToGeo(334368.634, 6250948.345, 56, 'H');
SELECT UTMToGeo(448251.6, 5411935.13, 31, CAST('U', 'Dynamic')) = UTMToGeo(448251.6, 5411935.13, 31, 1);
SELECT UTMToGeo(448251.6, 5411935.13, 31, CAST(1, 'Dynamic')) = UTMToGeo(448251.6, 5411935.13, 31, 'U');
SELECT isNull(g), g = UTMToGeo(448251.6, 5411935.13, 31, 1)
FROM (SELECT UTMToGeo(448251.6, 5411935.13, 31, band) AS g FROM values('band Dynamic', (NULL), ('U')));
