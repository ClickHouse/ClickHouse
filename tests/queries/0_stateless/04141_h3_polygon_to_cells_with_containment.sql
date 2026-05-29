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
SELECT arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCellsWithContainment(ring, resolution, 1)))
    = ['87283082bffffff'];

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCellsWithContainment(ring, resolution, 2)))
    = ['87283080cffffff','87283080dffffff','872830820ffffff','872830821ffffff','872830823ffffff','872830826ffffff','872830828ffffff','872830829ffffff','87283082affffff','87283082bffffff','87283082cffffff','87283082effffff','872830870ffffff','872830874ffffff','872830876ffffff'];

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCellsWithContainment(ring, resolution, 3)))
    = ['872830801ffffff','872830805ffffff','872830808ffffff','87283080cffffff','87283080dffffff','872830820ffffff','872830821ffffff','872830822ffffff','872830823ffffff','872830824ffffff','872830825ffffff','872830826ffffff','872830828ffffff','872830829ffffff','87283082affffff','87283082bffffff','87283082cffffff','87283082dffffff','87283082effffff','872830870ffffff','872830871ffffff','872830872ffffff','872830874ffffff','872830875ffffff','872830876ffffff','872830919ffffff','872830952ffffff','87283095affffff'];

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT
    arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCellsWithContainment(ring, resolution, 0)))
        != arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCellsWithContainment(ring, resolution, 1)))
    AND arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCellsWithContainment(ring, resolution, 1)))
        != arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCellsWithContainment(ring, resolution, 2)))
    AND arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCellsWithContainment(ring, resolution, 2)))
        != arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCellsWithContainment(ring, resolution, 3));

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

-- flags: native integer types other than UInt8/UInt32 use accurate cast to UInt32
WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT length(h3PolygonToCellsWithContainment(ring, resolution, toInt16(2)))
    = length(h3PolygonToCellsWithContainment(ring, resolution, 2));

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT length(h3PolygonToCellsWithContainment(ring, resolution, toUInt16(1)))
    = length(h3PolygonToCellsWithContainment(ring, resolution, 1));

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT length(h3PolygonToCellsWithContainment(materialize(ring), materialize(resolution), materialize(toInt32(0))))
    = length(h3PolygonToCellsWithContainment(ring, resolution, 0));

SELECT h3PolygonToCellsWithContainment(
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)],
    7,
    toInt8(-1)); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT h3PolygonToCellsWithContainment(
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)],
    7,
    toInt32(5)); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT h3PolygonToCellsWithContainment([(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)], 7, 4); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT h3PolygonToCellsWithContainment([(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)], 7); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Argument 1 = MultiLineString -> ILLEGAL_TYPE_OF_ARGUMENT.
SELECT h3PolygonToCellsWithContainment(
    CAST([[(1.0, 2.0), (3.0, 4.0)], [(5.0, 6.0), (7.0, 8.0)]], 'MultiLineString'),
    7,
    0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
