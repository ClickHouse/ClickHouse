-- Test MVTEncode aggregate state serialize/deserialize round-trip via a stored AggregateFunction column

DROP TABLE IF EXISTS mvt_state;
CREATE TABLE mvt_state (s AggregateFunction(MVTEncode('points'), Geometry, Tuple(cluster_count UInt64))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO mvt_state SELECT MVTEncodeState('points')((124.0, 3384.0)::Point::Geometry, tuple(toUInt64(3))::Tuple(cluster_count UInt64));
SELECT hex(MVTEncodeMerge('points')(s)) = hex(MVTEncode('points')((124.0, 3384.0)::Point::Geometry, tuple(toUInt64(3))::Tuple(cluster_count UInt64))) FROM mvt_state;
DROP TABLE mvt_state;

DROP TABLE IF EXISTS mvt_state_id;
CREATE TABLE mvt_state_id (s AggregateFunction(MVTEncode('t', 4096, 'fid'), Geometry, Tuple(fid UInt64, name String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO mvt_state_id SELECT MVTEncodeState('t', 4096, 'fid')((0.0, 0.0)::Point::Geometry, tuple(toUInt64(42), 'x')::Tuple(fid UInt64, name String));
SELECT hex(MVTEncodeMerge('t', 4096, 'fid')(s)) = hex(MVTEncode('t', 4096, 'fid')((0.0, 0.0)::Point::Geometry, tuple(toUInt64(42), 'x')::Tuple(fid UInt64, name String))) FROM mvt_state_id;
DROP TABLE mvt_state_id;
