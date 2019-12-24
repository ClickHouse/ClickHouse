#include "registerFunctions.h"

namespace DB
{
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
#endif
}

}
