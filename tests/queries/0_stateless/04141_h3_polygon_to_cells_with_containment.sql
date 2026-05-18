-- Tags: no-fasttest

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT
    length(exp) = length(orig) AND hasAll(exp, orig) AND hasAll(orig, exp)
FROM
(
    SELECT
        h3PolygonToCellsWithContainment(ring, resolution, 0) AS exp,
        h3PolygonToCells(ring, resolution) AS orig
);

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT length(h3PolygonToCellsWithContainment(ring, resolution, 1)) <= length(h3PolygonToCellsWithContainment(ring, resolution, 0));

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT length(h3PolygonToCellsWithContainment(ring, resolution, 2)) >= length(h3PolygonToCellsWithContainment(ring, resolution, 0));

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT length(h3PolygonToCellsWithContainment(ring, resolution, 3)) >= length(h3PolygonToCellsWithContainment(ring, resolution, 2));

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT length(h3PolygonToCellsWithContainment(materialize(ring), materialize(resolution), materialize(0))) > 0;


-- Empty MultiPolygon after a non-empty row must produce valid monotonic ColumnArray offsets (second row empty array).
WITH
    CAST([[[
        (-122.4089866999972145, 37.813318999983238),
        (-122.3544736999993603, 37.7198061999978478),
        (-122.4798767000009008, 37.8151571999998453),
        (-122.4089866999972145, 37.813318999983238)
    ]]], 'MultiPolygon') AS nonempty_mp,
    CAST([], 'MultiPolygon') AS empty_mp
SELECT countIf(cnt > 0) = 1 AND countIf(cnt = 0) = 1 AND count() = 2
FROM
(
    SELECT length(h3PolygonToCellsWithContainment(mp, 7, 0)) AS cnt
    FROM
    (
        SELECT arrayJoin([nonempty_mp, empty_mp]) AS mp
    )
);
WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    toUInt8(7) AS resolution
SELECT h3PolygonToCellsWithContainment(ring, resolution, 0.9); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT h3PolygonToCellsWithContainment([(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)], 7, 4); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT h3PolygonToCellsWithContainment([(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)], 7); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

