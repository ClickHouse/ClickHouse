#include <Functions/FunctionFactory.h>

#include "FunctionsUniqTheta.h"

#if USE_DATASKETCHES

namespace DB
{

REGISTER_FUNCTION(UniqTheta)
{
    factory.registerFunction<FunctionUniqThetaIntersect>();
    factory.registerFunction<FunctionUniqThetaUnion>();
    factory.registerFunction<FunctionUniqThetaNot>();
}

}

#endif
