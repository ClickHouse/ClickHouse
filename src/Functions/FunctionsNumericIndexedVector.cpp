#include <Functions/FunctionFactory.h>

// TODO include this last because of a broken roaring header. See the comment inside.
#include <Functions/FunctionsNumericIndexedVector.h>

namespace DB
{

REGISTER_FUNCTION(NumericIndexedVector)
{
    factory.registerFunction<FunctionNumericIndexedVectorAdd>();
    factory.registerFunction<FunctionNumericIndexedVectorCardinality>();
    factory.registerFunction<FunctionNumericIndexedVectorAllValueSum>();
    factory.registerFunction<FunctionNumericIndexedVectorShortDebugString>();
    factory.registerFunction<FunctionNumericIndexedVectorToMap>();
}
}
