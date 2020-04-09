#include "config_functions.h"

namespace DB
{

class FunctionFactory;

void registerFunctionGreatCircleDistance(FunctionFactory & factory);
void registerFunctionPointInEllipses(FunctionFactory & factory);
void registerFunctionPointInPolygon(FunctionFactory & factory);
void registerFunctionGeohashEncode(FunctionFactory & factory);
void registerFunctionGeohashDecode(FunctionFactory & factory);
void registerFunctionGeohashesInBox(FunctionFactory & factory);

#if USE_H3
void registerFunctionGeoToH3(FunctionFactory &);
#endif

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

