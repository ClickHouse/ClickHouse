-- Tags: no-fasttest

-- Test functions_h3_default_if_invalid setting.
-- With the default (false), h3 functions throw INCORRECT_DATA on invalid cell indices.
-- With the setting enabled (true), they return 0 or an empty array instead.

-- Default behavior: throw on invalid input
SELECT h3ToGeo(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3kRing(toUInt64(0), 1); -- { serverError INCORRECT_DATA }
SELECT h3ToChildren(toUInt64(0), 1); -- { serverError INCORRECT_DATA }
SELECT h3ToParent(toUInt64(0), 1); -- { serverError INCORRECT_DATA }
SELECT h3ToGeoBoundary(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3IsPentagon(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3IsResClassIII(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3ToCenterChild(toUInt64(0), 1); -- { serverError INCORRECT_DATA }

SELECT h3Distance(toUInt64(0), toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3Line(toUInt64(0), toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3GetFaces(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3GetUnidirectionalEdge(toUInt64(0), toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3GetUnidirectionalEdgesFromHexagon(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3IndexesAreNeighbors(toUInt64(0), toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3CellAreaM2(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3CellAreaRads2(toUInt64(0)); -- { serverError INCORRECT_DATA }

-- With setting enabled: return default values instead of throwing
SET functions_h3_default_if_invalid = true;

SELECT h3ToGeo(toUInt64(0));
SELECT h3kRing(toUInt64(0), 1);
SELECT h3ToChildren(toUInt64(0), 1);
SELECT h3ToParent(toUInt64(0), 1);
SELECT h3ToGeoBoundary(toUInt64(0));
SELECT h3IsPentagon(toUInt64(0));
SELECT h3IsResClassIII(toUInt64(0));
SELECT h3ToCenterChild(toUInt64(0), 1);
SELECT h3Distance(toUInt64(0), toUInt64(0));
SELECT h3Line(toUInt64(0), toUInt64(0));
SELECT h3GetFaces(toUInt64(0));
SELECT h3GetUnidirectionalEdge(toUInt64(0), toUInt64(0));
SELECT h3GetUnidirectionalEdgesFromHexagon(toUInt64(0));
SELECT h3IndexesAreNeighbors(toUInt64(0), toUInt64(0));
SELECT h3CellAreaM2(toUInt64(0));
SELECT h3CellAreaRads2(toUInt64(0));
