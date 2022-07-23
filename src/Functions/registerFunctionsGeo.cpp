#include "config_functions.h"

namespace DB
{

class FunctionFactory;

void registerFunctionGeoDistance(FunctionFactory & factory);
void registerFunctionPointInEllipses(FunctionFactory & factory);
void registerFunctionPointInPolygon(FunctionFactory & factory);
void registerFunctionPolygonsIntersection(FunctionFactory & factory);
void registerFunctionPolygonsUnion(FunctionFactory & factory);
void registerFunctionPolygonArea(FunctionFactory & factory);
void registerFunctionPolygonConvexHull(FunctionFactory & factory);
void registerFunctionPolygonsSymDifference(FunctionFactory & factory);
void registerFunctionPolygonsEquals(FunctionFactory & factory);
void registerFunctionPolygonsDistance(FunctionFactory & factory);
void registerFunctionPolygonsWithin(FunctionFactory & factory);
void registerFunctionPolygonPerimeter(FunctionFactory & factory);
void registerFunctionGeohashEncode(FunctionFactory & factory);
void registerFunctionGeohashDecode(FunctionFactory & factory);
void registerFunctionGeohashesInBox(FunctionFactory & factory);
void registerFunctionWkt(FunctionFactory & factory);
void registerFunctionReadWKT(FunctionFactory & factory);
void registerFunctionSvg(FunctionFactory & factory);

#if USE_H3
void registerFunctionGeoToH3(FunctionFactory &);
void registerFunctionH3ToGeo(FunctionFactory &);
void registerFunctionH3ToGeoBoundary(FunctionFactory &);
void registerFunctionH3EdgeAngle(FunctionFactory &);
void registerFunctionH3EdgeLengthM(FunctionFactory &);
void registerFunctionH3EdgeLengthKm(FunctionFactory &);
void registerFunctionH3ExactEdgeLengthM(FunctionFactory &);
void registerFunctionH3ExactEdgeLengthKm(FunctionFactory &);
void registerFunctionH3ExactEdgeLengthRads(FunctionFactory &);
void registerFunctionH3GetResolution(FunctionFactory &);
void registerFunctionH3IsValid(FunctionFactory &);
void registerFunctionH3KRing(FunctionFactory &);
void registerFunctionH3GetBaseCell(FunctionFactory &);
void registerFunctionH3ToParent(FunctionFactory &);
void registerFunctionH3ToChildren(FunctionFactory &);
void registerFunctionH3ToCenterChild(FunctionFactory &);
void registerFunctionH3IndexesAreNeighbors(FunctionFactory &);
void registerFunctionStringToH3(FunctionFactory &);
void registerFunctionH3ToString(FunctionFactory &);
void registerFunctionH3HexAreaM2(FunctionFactory &);
void registerFunctionH3IsResClassIII(FunctionFactory &);
void registerFunctionH3IsPentagon(FunctionFactory &);
void registerFunctionH3GetFaces(FunctionFactory &);
void registerFunctionH3HexAreaKm2(FunctionFactory &);
void registerFunctionH3CellAreaM2(FunctionFactory &);
void registerFunctionH3CellAreaRads2(FunctionFactory &);
void registerFunctionH3NumHexagons(FunctionFactory &);
void registerFunctionH3PointDistM(FunctionFactory &);
void registerFunctionH3PointDistKm(FunctionFactory &);
void registerFunctionH3PointDistRads(FunctionFactory &);
void registerFunctionH3GetRes0Indexes(FunctionFactory &);
void registerFunctionH3GetPentagonIndexes(FunctionFactory &);
void registerFunctionH3Line(FunctionFactory &);
void registerFunctionH3Distance(FunctionFactory &);
void registerFunctionH3HexRing(FunctionFactory &);
void registerFunctionH3GetUnidirectionalEdge(FunctionFactory &);
void registerFunctionH3UnidirectionalEdgeIsValid(FunctionFactory &);
void registerFunctionH3GetOriginIndexFromUnidirectionalEdge(FunctionFactory &);
void registerFunctionH3GetDestinationIndexFromUnidirectionalEdge(FunctionFactory &);
void registerFunctionH3GetIndexesFromUnidirectionalEdge(FunctionFactory &);
void registerFunctionH3GetUnidirectionalEdgesFromHexagon(FunctionFactory &);
void registerFunctionH3GetUnidirectionalEdgeBoundary(FunctionFactory &);
#endif

#if USE_S2_GEOMETRY
void registerFunctionGeoToS2(FunctionFactory &);
void registerFunctionS2ToGeo(FunctionFactory &);
void registerFunctionS2GetNeighbors(FunctionFactory &);
void registerFunctionS2CellsIntersect(FunctionFactory &);
void registerFunctionS2CapContains(FunctionFactory &);
void registerFunctionS2CapUnion(FunctionFactory &);
void registerFunctionS2RectAdd(FunctionFactory &);
void registerFunctionS2RectContains(FunctionFactory &);
void registerFunctionS2RectUnion(FunctionFactory &);
void registerFunctionS2RectIntersection(FunctionFactory &);
#endif


void registerFunctionsGeo(FunctionFactory & factory)
{
    registerFunctionGeoDistance(factory);
    registerFunctionPointInEllipses(factory);
    registerFunctionPointInPolygon(factory);
    registerFunctionPolygonsIntersection(factory);
    registerFunctionPolygonsUnion(factory);
    registerFunctionPolygonArea(factory);
    registerFunctionPolygonConvexHull(factory);
    registerFunctionPolygonsSymDifference(factory);
    registerFunctionPolygonsEquals(factory);
    registerFunctionPolygonsDistance(factory);
    registerFunctionPolygonsWithin(factory);
    registerFunctionPolygonPerimeter(factory);
    registerFunctionGeohashEncode(factory);
    registerFunctionGeohashDecode(factory);
    registerFunctionGeohashesInBox(factory);
    registerFunctionWkt(factory);
    registerFunctionReadWKT(factory);
    registerFunctionSvg(factory);

#if USE_H3
    registerFunctionGeoToH3(factory);
    registerFunctionH3ToGeo(factory);
    registerFunctionH3ToGeoBoundary(factory);
    registerFunctionH3EdgeAngle(factory);
    registerFunctionH3EdgeLengthM(factory);
    registerFunctionH3EdgeLengthKm(factory);
    registerFunctionH3ExactEdgeLengthM(factory);
    registerFunctionH3ExactEdgeLengthKm(factory);
    registerFunctionH3ExactEdgeLengthRads(factory);
    registerFunctionH3GetResolution(factory);
    registerFunctionH3IsValid(factory);
    registerFunctionH3KRing(factory);
    registerFunctionH3GetBaseCell(factory);
    registerFunctionH3ToParent(factory);
    registerFunctionH3ToChildren(factory);
    registerFunctionH3ToCenterChild(factory);
    registerFunctionH3IndexesAreNeighbors(factory);
    registerFunctionStringToH3(factory);
    registerFunctionH3ToString(factory);
    registerFunctionH3HexAreaM2(factory);
    registerFunctionH3IsResClassIII(factory);
    registerFunctionH3IsPentagon(factory);
    registerFunctionH3GetFaces(factory);
    registerFunctionH3HexAreaKm2(factory);
    registerFunctionH3CellAreaM2(factory);
    registerFunctionH3CellAreaRads2(factory);
    registerFunctionH3NumHexagons(factory);
    registerFunctionH3PointDistM(factory);
    registerFunctionH3PointDistKm(factory);
    registerFunctionH3PointDistRads(factory);
    registerFunctionH3GetRes0Indexes(factory);
    registerFunctionH3GetPentagonIndexes(factory);
    registerFunctionH3Line(factory);
    registerFunctionH3Distance(factory);
    registerFunctionH3HexRing(factory);
    registerFunctionH3GetUnidirectionalEdge(factory);
    registerFunctionH3UnidirectionalEdgeIsValid(factory);
    registerFunctionH3GetOriginIndexFromUnidirectionalEdge(factory);
    registerFunctionH3GetDestinationIndexFromUnidirectionalEdge(factory);
    registerFunctionH3GetIndexesFromUnidirectionalEdge(factory);
    registerFunctionH3GetUnidirectionalEdgesFromHexagon(factory);
    registerFunctionH3GetUnidirectionalEdgeBoundary(factory);
#endif

#if USE_S2_GEOMETRY
    registerFunctionGeoToS2(factory);
    registerFunctionS2ToGeo(factory);
    registerFunctionS2GetNeighbors(factory);
    registerFunctionS2CellsIntersect(factory);
    registerFunctionS2CapContains(factory);
    registerFunctionS2CapUnion(factory);
    registerFunctionS2RectAdd(factory);
    registerFunctionS2RectContains(factory);
    registerFunctionS2RectUnion(factory);
    registerFunctionS2RectIntersection(factory);
#endif
}

}
