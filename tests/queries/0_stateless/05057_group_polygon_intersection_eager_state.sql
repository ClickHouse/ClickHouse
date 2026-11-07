-- A `NonEmpty` intersection state is always a fully reduced running intersection. Once two
-- inputs are disjoint, direct aggregation, state merging, and compatibility deserialization must
-- all persist the absorbing `Empty` mode (`01` version + `02` mode) rather than two pending chunks.

SELECT 'direct_state_serializes_empty';
SELECT hex(groupPolygonIntersectionState(p)) = '0102'
FROM
(
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'),
        readWKTPolygon('POLYGON ((10 10, 10 11, 11 11, 11 10, 10 10))')
    ]) AS p
)
SETTINGS max_threads = 1;

SELECT 'merged_state_serializes_empty';
SELECT hex(groupPolygonIntersectionMergeState(state)) = '0102'
FROM
(
    SELECT groupPolygonIntersectionState(p) AS state
    FROM
    (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p
    )
    UNION ALL
    SELECT groupPolygonIntersectionState(p) AS state
    FROM
    (
        SELECT readWKTPolygon('POLYGON ((10 10, 10 11, 11 11, 11 10, 10 10))') AS p
    )
);

SELECT 'legacy_two_chunk_state_reduces_on_read';
WITH
    hex(groupPolygonIntersectionState(
        readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'))) AS state1,
    hex(groupPolygonIntersectionState(
        readWKTPolygon('POLYGON ((10 10, 10 11, 11 11, 11 10, 10 10))'))) AS state2
SELECT hex(CAST(
    unhex(concat('010102', substring(state1, 7), substring(state2, 7)))
    AS AggregateFunction(groupPolygonIntersection, Polygon))) = '0102';
