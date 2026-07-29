-- Tags: no-fasttest
-- no-fasttest: h3ToGeo/geoToH3 need a binary built with the Uber H3 library

-- h3togeo_lon_lat_result_order (default 0): h3ToGeo returns Tuple(latitude, longitude) by default and
-- Tuple(longitude, latitude) when set to 1. Unlike the settings above this keeps the SAME top-level type
-- Tuple(Float64, Float64); it only SWAPS the two elements, so a poisoned recompute does not abort with a
-- Bad cast - it silently reorders the key tuple. The observable divergence (parts serialized under one
-- element order, KeyDescription::data_types recomputed under the other) only appears across a
-- version/global-default boundary; a single self-consistent binary always serializes and deserializes
-- primary.idx under the same effective order, so this case cannot fail-without/pass-with on one binary.
-- It is a behavior-preserving guard: CREATE under a transient session flip must be neutralized to the
-- canonical (latitude, longitude) order, so parts are stored latitude-major and _part_offset order (the
-- physical stored order, independent of the reader's session setting) is monotonic in latitude.
DROP TABLE IF EXISTS th3;
SET h3togeo_lon_lat_result_order = 1;
CREATE TABLE th3 (h UInt64) ENGINE = MergeTree() ORDER BY h3ToGeo(h) SETTINGS index_granularity = 1;
SET h3togeo_lon_lat_result_order = 0;
INSERT INTO th3 SELECT geoToH3(lon, lat, 5) FROM values('lat Float64, lon Float64', (10., 60.), (80., 5.), (-40., 50.), (50., -120.));
SELECT round(h3ToGeo(h).1, 1) FROM th3 ORDER BY _part_offset SETTINGS max_threads = 1;

