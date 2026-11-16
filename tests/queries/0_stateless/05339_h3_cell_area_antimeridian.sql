-- Tags: no-fasttest
-- https://github.com/ClickHouse/ClickHouse/issues/124447
-- Since h3 4.5.0 cellAreaRads2 sums Cagnoli terms over the cell edges. For an edge crossing the
-- antimeridian the raw longitude difference is close to 2*pi and the tiny true step is lost to its
-- ulp, so the area of a straddling cell at resolutions >= 12 was up to 7.8% off from its neighbour.

SELECT
    h3GetResolution(c) AS res,
    round(h3CellAreaM2(c) / h3CellAreaM2(n), 4) AS m2_ratio,
    round(h3CellAreaRads2(c) / h3CellAreaRads2(n), 4) AS rads2_ratio
FROM values('c UInt64, n UInt64',
    (635102696247273663, 635102696247274879),
    (640403264882466919, 640403264882467167),
    (646257791391718301, 646257791391718300))
ORDER BY res;
