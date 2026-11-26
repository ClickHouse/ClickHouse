-- Tags: no-fasttest

-- Test that newly validated H3 functions throw on invalid cell/edge indices.

-- Cell functions: h3GetBaseCell, h3GetResolution
SELECT h3GetBaseCell(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3GetResolution(toUInt64(0)); -- { serverError INCORRECT_DATA }

-- Edge functions: throw on invalid directed edge indices
SELECT h3ExactEdgeLengthKm(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3ExactEdgeLengthM(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3ExactEdgeLengthRads(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3GetOriginIndexFromUnidirectionalEdge(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3GetDestinationIndexFromUnidirectionalEdge(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3GetIndexesFromUnidirectionalEdge(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3GetUnidirectionalEdgeBoundary(toUInt64(0)); -- { serverError INCORRECT_DATA }

-- With functions_h3_default_if_invalid = true: return defaults instead of throwing
SET functions_h3_default_if_invalid = true;

SELECT h3GetBaseCell(toUInt64(0));
SELECT h3GetResolution(toUInt64(0));
SELECT h3ExactEdgeLengthKm(toUInt64(0));
SELECT h3ExactEdgeLengthM(toUInt64(0));
SELECT h3ExactEdgeLengthRads(toUInt64(0));
SELECT h3GetOriginIndexFromUnidirectionalEdge(toUInt64(0));
SELECT h3GetDestinationIndexFromUnidirectionalEdge(toUInt64(0));
SELECT h3GetIndexesFromUnidirectionalEdge(toUInt64(0));
SELECT h3GetUnidirectionalEdgeBoundary(toUInt64(0));

-- Valid inputs still work
SET functions_h3_default_if_invalid = false;
SELECT h3GetBaseCell(612916788725809151);
SELECT h3GetResolution(612916788725809151);
SELECT round(h3ExactEdgeLengthKm(1310277011704381439), 2);
