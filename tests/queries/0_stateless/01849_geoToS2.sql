-- Tags: no-fasttest
-- Tag no-fasttest: needs s2

DROP TABLE IF EXISTS s2_indexes;

CREATE TABLE s2_indexes (s2_index UInt64, longitude Float64, latitude Float64) ENGINE = Memory;

-- Random geo coordinates were generated using S2Testing::RandomPoint() method from s2 API.

INSERT INTO s2_indexes VALUES (3814912406305146967, 125.938503, 25.519362);
INSERT INTO s2_indexes VALUES (10654167528317613967, -64.364998, -13.206226);
INSERT INTO s2_indexes VALUES (1913723177026859705, 8.774109, -3.271374);
INSERT INTO s2_indexes VALUES (13606307743304496111, -89.810962, -57.013984);
INSERT INTO s2_indexes VALUES (8094352344009072761,-170.423649, -10.102188);
INSERT INTO s2_indexes VALUES (2414200527355011659, 54.724353, -19.210608);
INSERT INTO s2_indexes VALUES (4590287096029015693, 51.390374, 29.368252);
INSERT INTO s2_indexes VALUES (10173921221664598133, 5.161979, -46.718242);
INSERT INTO s2_indexes VALUES (525948609053546189, -41.564128, -16.777073);
INSERT INTO s2_indexes VALUES (2446780491369950853, 49.94229, -18.633856);
INSERT INTO s2_indexes VALUES (1723620528513492581, 40.768274, 2.853563);
INSERT INTO s2_indexes VALUES (8295275405228383207, -168.258979, -38.271170);
INSERT INTO s2_indexes VALUES (7280210779810727639, 170.145748, 7.606449);
INSERT INTO s2_indexes VALUES (10670400906708524495, -61.761938, -24.969589);
INSERT INTO s2_indexes VALUES (10868726821406046149, -79.245460, -22.940849);
INSERT INTO s2_indexes VALUES (13202270384266773545, 10.610774, -64.184103);
INSERT INTO s2_indexes VALUES (145638248314527629, -19.826140, -41.192912);
INSERT INTO s2_indexes VALUES (12793606480989360601, 74.006104, -68.321240);
INSERT INTO s2_indexes VALUES (6317132534461540391, -165.907973, 54.205178);
INSERT INTO s2_indexes VALUES (6944470717485986643, 140.428834, 28.399755);

SELECT 'Checking s2 index generation.';

SELECT s2ToGeo(s2_index), geoToS2(longitude, latitude) FROM s2_indexes ORDER BY s2_index;

SELECT first, second, result FROM (
    SELECT
        s2ToGeo(geoToS2(longitude, latitude)) AS output_geo,
        tuple(roundBankers(longitude, 5), roundBankers(latitude, 5)) AS first,
        tuple(roundBankers(output_geo.1, 5), roundBankers(output_geo.2, 5)) AS second,
        if(first = second, 'ok', 'fail') AS result
    FROM s2_indexes
    ORDER BY s2_index
 );

SELECT s2ToGeo(toUInt64(-1)); -- { serverError 36 }
SELECT s2ToGeo(nan); -- { serverError 43 }
SELECT geoToS2(toFloat64(toUInt64(-1)), toFloat64(toUInt64(-1)));
SELECT geoToS2(nan, nan); -- { serverError 43 }
SELECT geoToS2(-inf, 1.1754943508222875e-38); -- { serverError 43 }



DROP TABLE IF EXISTS s2_indexes;
