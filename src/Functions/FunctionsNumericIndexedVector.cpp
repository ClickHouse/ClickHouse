#include <Functions/FunctionFactory.h>

// TODO include this last because of a broken roaring header. See the comment inside.
#include <Functions/FunctionsNumericIndexedVector.h>

namespace DB
{

REGISTER_FUNCTION(NumericIndexedVector)
{
    factory.registerFunction<FunctionNumericIndexedVectorBuild>();
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseAdd>();
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseSubtract>();
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseMultiply>();
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseDivide>();
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseEqual>();
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseNotEqual>();
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseLess>();
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseLessEqual>();
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseGreater>();
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseGreaterEqual>();
    factory.registerFunction<FunctionNumericIndexedVectorGetValueImpl>();
    factory.registerFunction<FunctionNumericIndexedVectorCardinality>();
    factory.registerFunction<FunctionNumericIndexedVectorAllValueSum>();
    factory.registerFunction<FunctionNumericIndexedVectorShortDebugString>();
    factory.registerFunction<FunctionNumericIndexedVectorToMap>();
}
}
