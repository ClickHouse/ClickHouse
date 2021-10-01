#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

namespace DB
{

class FunctionFactory;

void registerFunctionGeoDistance(FunctionFactory & factory);
void registerFunctionPointInEllipses(FunctionFactory & factory);
void registerFunctionPointInPolygon(FunctionFactory & factory);
void registerFunctionGeohashEncode(FunctionFactory & factory);
void registerFunctionGeohashDecode(FunctionFactory & factory);
void registerFunctionGeohashesInBox(FunctionFactory & factory);

#if USE_H3
void registerFunctionGeoToH3(FunctionFactory &);
void registerFunctionH3EdgeAngle(FunctionFactory &);
void registerFunctionH3EdgeLengthM(FunctionFactory &);
void registerFunctionH3GetResolution(FunctionFactory &);
void registerFunctionH3IsValid(FunctionFactory &);
void registerFunctionH3KRing(FunctionFactory &);
void registerFunctionH3GetBaseCell(FunctionFactory &);
void registerFunctionH3ToParent(FunctionFactory &);
void registerFunctionH3ToChildren(FunctionFactory &);
void registerFunctionH3IndexesAreNeighbors(FunctionFactory &);
void registerFunctionStringToH3(FunctionFactory &);
void registerFunctionH3ToString(FunctionFactory &);
void registerFunctionH3HexAreaM2(FunctionFactory &);
#endif


void registerFunctionsGeo(FunctionFactory & factory)
{
    registerFunctionGeoDistance(factory);
    registerFunctionPointInEllipses(factory);
    registerFunctionPointInPolygon(factory);
    registerFunctionGeohashEncode(factory);
    registerFunctionGeohashDecode(factory);
    registerFunctionGeohashesInBox(factory);

#if USE_H3
    registerFunctionGeoToH3(factory);
    registerFunctionH3EdgeAngle(factory);
    registerFunctionH3EdgeLengthM(factory);
    registerFunctionH3GetResolution(factory);
    registerFunctionH3IsValid(factory);
    registerFunctionH3KRing(factory);
    registerFunctionH3GetBaseCell(factory);
    registerFunctionH3ToParent(factory);
    registerFunctionH3ToChildren(factory);
    registerFunctionH3IndexesAreNeighbors(factory);
    registerFunctionStringToH3(factory);
    registerFunctionH3ToString(factory);
    registerFunctionH3HexAreaM2(factory);
#endif
}

}
