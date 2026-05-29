-- Tags: no-fasttest

WITH
    [(-122.40898669999721, 37.81331899998324), (-122.35447369999936, 37.71980619999785), (-122.4798767000009, 37.815157199999845)] AS ring,
    7 AS res,
    arrayDistinct(h3PolygonToCellsExperimental(ring, res, 'center')) AS center,
    arrayDistinct(h3PolygonToCellsExperimental(ring, res, 'full')) AS full,
    arrayDistinct(h3PolygonToCellsExperimental(ring, res, 'overlapping')) AS overlapping,
    arrayDistinct(h3PolygonToCellsExperimental(ring, res, 'overlapping_bbox')) AS overlapping_bbox
SELECT
    arrayAll(x -> has(center, x), full),
    arrayAll(x -> has(overlapping, x), center),
    arrayAll(x -> has(overlapping_bbox, x), overlapping),
    (length(full) <= length(center)) AND (length(center) <= length(overlapping)) AND (length(overlapping) <= length(overlapping_bbox));

WITH
    [(-122.40898669999721, 37.81331899998324), (-122.35447369999936, 37.71980619999785), (-122.4798767000009, 37.815157199999845)] AS ring,
    7 AS res,
    arrayDistinct(h3PolygonToCellsExperimental(ring, res, 'center')) AS center
SELECT
    length(h3PolygonToCellsExperimental(CAST(ring, 'Ring'), res, 'center')) = length(center),
    length(h3PolygonToCellsExperimental(CAST([ring], 'Polygon'), res, 'center')) > 0,
    length(h3PolygonToCellsExperimental(CAST([[ring]], 'MultiPolygon'), res, 'center')) > 0;

WITH ['center', 'full'] AS modes
SELECT h3PolygonToCellsExperimental([(-122.40898669999721, 37.81331899998324), (-122.35447369999936, 37.71980619999785), (-122.4798767000009, 37.815157199999845)], 7, arrayJoin(modes)); -- { serverError ILLEGAL_COLUMN }

SELECT h3PolygonToCellsExperimental([(-122.40898669999721, 37.81331899998324), (-122.35447369999936, 37.71980619999785), (-122.4798767000009, 37.815157199999845)], 7, 'bad'); -- { serverError BAD_ARGUMENTS }

SELECT h3PolygonToCellsExperimental(CAST((0., 0.), 'Point'), 7, 'center'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT h3PolygonToCellsExperimental(CAST([(0., 0.), (1., 1.)], 'LineString'), 7, 'center'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
