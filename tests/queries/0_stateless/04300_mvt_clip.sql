-- Tests for the mvtEncode clip buffer parameter and the tile bounding-box helpers mvtTileBBox / mvtTileBBoxMercator.

SELECT '-- mvtTileBBox: bounding box of the whole world at zoom 0';
SELECT mvtTileBBox(0, 0, 0);

SELECT '-- mvtTileBBoxMercator: bounding box of a tile in Web Mercator space';
SELECT mvtTileBBoxMercator(1, 0, 0);

SELECT '-- mvtTileBBox: a positive margin expands the box on every side';
WITH mvtTileBBox(10, 550, 335) AS bb, mvtTileBBox(10, 550, 335, 0.1) AS bm
SELECT bm.1 < bb.1, bm.2 < bb.2, bm.3 > bb.3, bm.4 > bb.4;

SELECT '-- mvtTileBBox round-trips with mvtEncodeGeom: the tile centre projects to an interior pixel';
WITH mvtTileBBox(10, 550, 335) AS bb
SELECT mvtEncodeGeom((bb.1 + bb.3) / 2, (bb.2 + bb.4) / 2, 10, 550, 335);

SELECT '-- mvtTileBBox round-trips with mvtEncodeGeom: a point inside the box projects within [0, extent]';
WITH
    mvtTileBBox(10, 550, 335) AS bb,
    mvtEncodeGeom(bb.1 + (bb.3 - bb.1) * 0.25, bb.2 + (bb.4 - bb.2) * 0.25, 10, 550, 335) AS px
SELECT (px.1 BETWEEN 0 AND 4096) AND (px.2 BETWEEN 0 AND 4096);

DROP TABLE IF EXISTS mvt_clip_pts;
CREATE TABLE mvt_clip_pts (lon Float64, lat Float64) ENGINE = Memory;
INSERT INTO mvt_clip_pts VALUES (13.37, 52.52) (13.37, 52.52) (0, 0);

SELECT '-- mvtEncode: the buffer parameter clips out-of-tile features (only the Berlin cluster of 2 survives)';
SELECT hex(mvtEncode('points', 4096, 64)(geom, tuple(cnt)::Tuple(cluster_count UInt64)))
FROM (SELECT mvtEncodeGeom(lon, lat, 10, 550, 335) AS geom, count() AS cnt FROM mvt_clip_pts GROUP BY geom);

SELECT '-- mvtEncode: without a buffer nothing is clipped, so the out-of-tile point is also encoded';
SELECT
    length(mvtEncode('points', 4096, 64)(geom, tuple(cnt)::Tuple(cluster_count UInt64))) AS clipped_len,
    length(mvtEncode('points')(geom, tuple(cnt)::Tuple(cluster_count UInt64))) AS unclipped_len,
    clipped_len < unclipped_len AS clip_removed_features
FROM (SELECT mvtEncodeGeom(lon, lat, 10, 550, 335) AS geom, count() AS cnt FROM mvt_clip_pts GROUP BY geom);

SELECT '-- mvtEncode: buffer 0 clips a point ~5px past the edge that buffer 16 keeps';
WITH
    mvtTileBBox(10, 550, 335) AS bb,
    (bb.3 - bb.1) / 4096 AS deg_per_px,
    bb.3 + deg_per_px * 5 AS lon_just_outside,
    (bb.2 + bb.4) / 2 AS lat_mid
SELECT
    length(mvtEncode('points', 4096, 0)(mvtEncodeGeom(lon_just_outside, lat_mid, 10, 550, 335))) AS buf0_len,
    length(mvtEncode('points', 4096, 16)(mvtEncodeGeom(lon_just_outside, lat_mid, 10, 550, 335))) > 0 AS buf16_nonempty;

SELECT '-- mvtEncode: a group entirely outside the tile yields an empty tile';
SELECT length(mvtEncode('points', 4096, 64)(geom, tuple(toUInt64(1))::Tuple(cluster_count UInt64)))
FROM (SELECT mvtEncodeGeom(lon, lat, 10, 550, 335) AS geom FROM mvt_clip_pts WHERE lon = 0);

SELECT '-- mvtEncode: clipping leaves the result a String';
SELECT toTypeName(mvtEncode('points', 4096, 64)(geom))
FROM (SELECT mvtEncodeGeom(lon, lat, 10, 550, 335) AS geom FROM mvt_clip_pts);

SELECT '-- mvtEncode: an out-of-range buffer parameter is rejected';
SELECT mvtEncode('points', 4096, -1)(mvtEncodeGeom(13.37, 52.52, 10, 550, 335)); -- { serverError BAD_ARGUMENTS }

DROP TABLE mvt_clip_pts;
