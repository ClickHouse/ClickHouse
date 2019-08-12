#include "config_functions.h"
#include <Functions/registerFunctions.h>

namespace DB
{
void registerFunctionsGeo(FunctionFactory & factory)
{
    registerFunctionGreatCircleDistance(factory);
    registerFunctionPointInEllipses(factory);
    registerFunctionPointInPolygon(factory);
    registerFunctionGeohashEncode(factory);
    registerFunctionGeohashDecode(factory);
    registerFunctionGeohashesInBox(factory);

#if USE_H3
    registerFunctionGeoToH3(factory);
#endif
}

}
