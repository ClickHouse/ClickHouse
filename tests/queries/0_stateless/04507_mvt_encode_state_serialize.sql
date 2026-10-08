-- Test MVTEncode aggregate state serialize/deserialize round-trip via a stored AggregateFunction column.
-- The merged results are compared against golden tile bytes (the same literals as in 04299_mvt_encode)
-- instead of re-evaluating `MVTEncode` on a constant `::Point::Geometry` expression, because with
-- parallel replicas the initiator constant-folds such a cast and re-serializes it as
-- `_CAST((0., 0.), 'Geometry')`, which the replica rejects, see https://github.com/ClickHouse/ClickHouse/issues/74366.

DROP TABLE IF EXISTS mvt_state;
CREATE TABLE mvt_state (s AggregateFunction(MVTEncode('points'), Geometry, Tuple(cluster_count UInt64))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO mvt_state SELECT MVTEncodeState('points')((124.0, 3384.0)::Point::Geometry, tuple(toUInt64(3))::Tuple(cluster_count UInt64));
SELECT hex(MVTEncodeMerge('points')(s)) FROM mvt_state;
DROP TABLE mvt_state;

DROP TABLE IF EXISTS mvt_state_id;
CREATE TABLE mvt_state_id (s AggregateFunction(MVTEncode('t', 4096, 'fid'), Geometry, Tuple(fid UInt64, name String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO mvt_state_id SELECT MVTEncodeState('t', 4096, 'fid')((0.0, 0.0)::Point::Geometry, tuple(toUInt64(42), 'x')::Tuple(fid UInt64, name String));
SELECT hex(MVTEncodeMerge('t', 4096, 'fid')(s)) FROM mvt_state_id;
DROP TABLE mvt_state_id;
