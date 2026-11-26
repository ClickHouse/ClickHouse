-- Tests for UTM and MGRS coordinate functions: geoToUTM, UTMToGeo, geoToMGRS, MGRSToGeo.

SELECT '-- geoToUTM: forward conversion';
WITH geoToUTM(2.294497, 48.858222) AS t SELECT round(t.1, 3), round(t.2, 3), t.3, t.4;     -- Eiffel Tower, France (31U)
WITH geoToUTM(55.2744, 25.1972) AS t SELECT round(t.1, 3), round(t.2, 3), t.3, t.4;        -- Burj Khalifa, Dubai (40R)
WITH geoToUTM(-122.4194, 37.7749) AS t SELECT round(t.1, 3), round(t.2, 3), t.3, t.4;      -- San Francisco (10S)
WITH geoToUTM(151.2093, -33.8688) AS t SELECT round(t.1, 3), round(t.2, 3), t.3, t.4;      -- Sydney, southern hemisphere (56H)
WITH geoToUTM(0., 0.) AS t SELECT round(t.1, 3), round(t.2, 3), t.3, t.4;                  -- equator / prime meridian (31N)

SELECT '-- geoToUTM: Norway and Svalbard zone exceptions';
SELECT geoToUTM(4.0, 60.0).3 AS norway_zone;       -- 32; the normal 6-degree formula would give 31 here
SELECT geoToUTM(20.0, 78.0).3 AS svalbard_zone;    -- 33; the normal formula would give 34 here
SELECT geoToUTM(6.0, 84.0).3 AS svalbard_top_zone; -- 31 at the inclusive 84 degree boundary; normal formula gives 32

SELECT '-- geoToUTM: forced zone';
WITH geoToUTM(2.294497, 48.858222, 32) AS t SELECT round(t.1, 3), round(t.2, 3), t.3, t.4; -- project into a neighbouring zone

SELECT '-- UTMToGeo: inverse conversion round-trips back to the input (within 1e-6 degrees)';
WITH (2.294497, 48.858222) AS p, geoToUTM(p.1, p.2) AS u, UTMToGeo(u.1, u.2, u.3, u.4 >= 'N') AS g
    SELECT abs(g.1 - p.1) < 1e-6 AND abs(g.2 - p.2) < 1e-6;
WITH (55.2744, 25.1972) AS p, geoToUTM(p.1, p.2) AS u, UTMToGeo(u.1, u.2, u.3, u.4 >= 'N') AS g
    SELECT abs(g.1 - p.1) < 1e-6 AND abs(g.2 - p.2) < 1e-6;
WITH (151.2093, -33.8688) AS p, geoToUTM(p.1, p.2) AS u, UTMToGeo(u.1, u.2, u.3, u.4 >= 'N') AS g
    SELECT abs(g.1 - p.1) < 1e-6 AND abs(g.2 - p.2) < 1e-6;

SELECT '-- geoToMGRS: encoding at several precisions';
SELECT geoToMGRS(2.294497, 48.858222);       -- default precision (1 m)
SELECT geoToMGRS(2.294497, 48.858222, 5);
SELECT geoToMGRS(2.294497, 48.858222, 3);    -- 100 m
SELECT geoToMGRS(2.294497, 48.858222, 0);    -- grid square only
SELECT geoToMGRS(55.2744, 25.1972);          -- Dubai
SELECT geoToMGRS(151.2093, -33.8688);        -- Sydney, southern hemisphere

SELECT '-- MGRSToGeo: decoding to the centre of the grid square';
WITH MGRSToGeo('31UDQ4825111935') AS g SELECT round(g.1, 5), round(g.2, 5);
SELECT MGRSToGeo('31UDQ4825111935') = MGRSToGeo('31u dq 48251 11935');  -- whitespace and case insensitive
SELECT MGRSToGeo(toFixedString('31UDQ4825111935', 20)) = MGRSToGeo('31UDQ4825111935');  -- null-padded FixedString decodes identically

SELECT '-- MGRSToGeo(geoToMGRS(...)) re-encodes to the same string';
SELECT geoToMGRS(MGRSToGeo('31UDQ4825111935').1, MGRSToGeo('31UDQ4825111935').2) = '31UDQ4825111935';

SELECT '-- vectorized over a table';
SELECT geoToMGRS(lon, lat, 4) FROM values('lon Float64, lat Float64', (2.294497, 48.858222), (55.2744, 25.1972), (151.2093, -33.8688)) ORDER BY lon;

SELECT '-- error handling';
SELECT geoToUTM(0., 85.); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT geoToUTM(0., -81.); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT geoToUTM(181., 0.); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT geoToUTM(0., 0., 0); -- { serverError BAD_ARGUMENTS }
SELECT geoToUTM(0., 0., 61); -- { serverError BAD_ARGUMENTS }
SELECT UTMToGeo(500000., 0., 0, 1); -- { serverError BAD_ARGUMENTS }
SELECT MGRSToGeo('hello'); -- { serverError BAD_ARGUMENTS }
SELECT MGRSToGeo('31'); -- { serverError BAD_ARGUMENTS }
SELECT MGRSToGeo('31UDQ482111935'); -- { serverError BAD_ARGUMENTS }
SELECT MGRSToGeo('31UDQ482511193512'); -- { serverError BAD_ARGUMENTS } -- more than five digits per coordinate
SELECT MGRSToGeo('31UQD4825111935'); -- { serverError BAD_ARGUMENTS } -- column letter out of range for the zone
SELECT MGRSToGeo('32XAA'); -- { serverError BAD_ARGUMENTS } -- zone 32 does not exist in the X band
SELECT MGRSToGeo('1LAB'); -- { serverError BAD_ARGUMENTS } -- row letter B does not intersect latitude band L
SELECT MGRSToGeo('031UDQ4825111935'); -- { serverError BAD_ARGUMENTS } -- zone designator more than two digits
SELECT UTMToGeo(448251.6, 5411935.13, 31, 2); -- { serverError BAD_ARGUMENTS } -- is_north must be 0 or 1
SELECT geoToMGRS(0., 0., 6); -- { serverError ARGUMENT_OUT_OF_BOUND } -- precision above 5
SELECT geoToMGRS(0., 0., -1); -- { serverError ARGUMENT_OUT_OF_BOUND } -- negative precision
