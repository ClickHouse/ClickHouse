-- Tags: no-fasttest

WITH 7 AS resolution,
    [(-122.40898669999721, 37.81331899998324), (-122.35447369999936, 37.71980619999785), (-122.4798767000009, 37.815157199999845)] AS ring,
    ['872830820ffffff', '872830828ffffff', '87283082affffff', '87283082bffffff', '87283082effffff', '872830870ffffff', '872830876ffffff'] AS reference
SELECT h3PolygonToCells(ring, resolution), arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCells(ring, resolution))) = reference;

WITH 7 AS resolution,
    [(-122.40898669999721, 37.81331899998324), (-122.35447369999936, 37.71980619999785), (-122.4798767000009, 37.815157199999845)] AS ring,
    ['872830820ffffff', '872830828ffffff', '87283082affffff', '87283082bffffff', '87283082effffff', '872830870ffffff', '872830876ffffff'] AS reference
SELECT h3PolygonToCells(ring, materialize(resolution)), arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCells(ring, materialize(resolution)))) = reference;

WITH 7 AS resolution,
    [(-122.40898669999721, 37.81331899998324), (-122.35447369999936, 37.71980619999785), (-122.4798767000009, 37.815157199999845)] AS ring,
    ['872830820ffffff', '872830828ffffff', '87283082affffff', '87283082bffffff', '87283082effffff', '872830870ffffff', '872830876ffffff'] AS reference
SELECT h3PolygonToCells(materialize(ring), resolution), arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCells(materialize(ring), resolution))) = reference;

WITH 7 AS resolution,
    [(-122.40898669999721, 37.81331899998324), (-122.35447369999936, 37.71980619999785), (-122.4798767000009, 37.815157199999845)] AS ring,
    ['872830820ffffff', '872830828ffffff', '87283082affffff', '87283082bffffff', '87283082effffff', '872830870ffffff', '872830876ffffff'] AS reference
SELECT h3PolygonToCells(materialize(ring), materialize(resolution)), arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCells(materialize(ring), materialize(resolution)))) = reference;

WITH range(0, 8) AS resolutions,
    [(-122.40898669999721, 37.81331899998324), (-122.35447369999936, 37.71980619999785), (-122.4798767000009, 37.815157199999845)] AS ring
SELECT resolution, h3PolygonToCells(ring, arrayJoin(resolutions) AS resolution)
ORDER BY resolution;

DROP TABLE IF EXISTS rings;
CREATE TABLE rings (ring Ring, reference Array(String)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO rings SELECT
    [(-122.40898669999721, 37.81331899998324), (-122.35447369999936, 37.71980619999785), (-122.4798767000009, 37.815157199999845)],
    ['872830820ffffff', '872830828ffffff', '87283082affffff', '87283082bffffff', '87283082effffff', '872830870ffffff', '872830876ffffff']
FROM numbers(10000);

SELECT DISTINCT h3PolygonToCells(ring, 7), arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCells(ring, 7))) = reference FROM rings;

WITH range(0, 8) AS resolutions
SELECT DISTINCT resolution, h3PolygonToCells(ring, arrayJoin(resolutions) AS resolution) FROM rings
ORDER BY resolution;

DROP TABLE rings;
