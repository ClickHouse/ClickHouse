-- Tags: no-fasttest

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT
    length(exp) = length(orig) AND hasAll(exp, orig) AND hasAll(orig, exp)
FROM
(
    SELECT
        h3PolygonToCellsExperimental(ring, resolution, 0) AS exp,
        h3PolygonToCells(ring, resolution) AS orig
);

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT length(h3PolygonToCellsExperimental(ring, resolution, 1)) <= length(h3PolygonToCellsExperimental(ring, resolution, 0));

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT length(h3PolygonToCellsExperimental(ring, resolution, 2)) >= length(h3PolygonToCellsExperimental(ring, resolution, 0));

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT length(h3PolygonToCellsExperimental(ring, resolution, 3)) >= length(h3PolygonToCellsExperimental(ring, resolution, 2));

WITH
    [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS ring,
    7 AS resolution
SELECT length(h3PolygonToCellsExperimental(materialize(ring), materialize(resolution), materialize(0))) > 0;

-- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT h3PolygonToCellsExperimental([(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)], 7, 4);

-- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT h3PolygonToCellsExperimental([(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)], 7);
