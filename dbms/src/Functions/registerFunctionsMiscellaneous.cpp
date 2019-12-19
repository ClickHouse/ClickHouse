#include <Functions/array/registerFunctionsArray.h>
#include "registerFunctions.h"

namespace DB
{
void registerFunctionsMiscellaneous(FunctionFactory & factory)
{
    registerFunctionCurrentDatabase(factory);
    registerFunctionCurrentUser(factory);
    registerFunctionCurrentQuota(factory);
    registerFunctionHostName(factory);
    registerFunctionFQDN(factory);
    registerFunctionVisibleWidth(factory);
    registerFunctionToTypeName(factory);
    registerFunctionGetSizeOfEnumType(factory);
    registerFunctionToColumnTypeName(factory);
    registerFunctionDumpColumnStructure(factory);
    registerFunctionDefaultValueOfArgumentType(factory);
    registerFunctionBlockSize(factory);
    registerFunctionBlockNumber(factory);
    registerFunctionRowNumberInBlock(factory);
    registerFunctionRowNumberInAllBlocks(factory);
    registerFunctionNeighbor(factory);
    registerFunctionSleep(factory);
    registerFunctionSleepEachRow(factory);
    registerFunctionMaterialize(factory);
    registerFunctionIgnore(factory);
    registerFunctionIgnoreExceptNull(factory);
    registerFunctionIndexHint(factory);
    registerFunctionIdentity(factory);
    registerFunctionArrayJoin(factory);
    registerFunctionReplicate(factory);
    registerFunctionBar(factory);
    registerFunctionHasColumnInTable(factory);
    registerFunctionIsFinite(factory);
    registerFunctionIsInfinite(factory);
    registerFunctionIsNaN(factory);
    registerFunctionThrowIf(factory);
    registerFunctionVersion(factory);
    registerFunctionUptime(factory);
    registerFunctionTimeZone(factory);
    registerFunctionRunningAccumulate(factory);
    registerFunctionRunningDifference(factory);
    registerFunctionRunningDifferenceStartingWithFirstValue(factory);
    registerFunctionFinalizeAggregation(factory);
    registerFunctionToLowCardinality(factory);
    registerFunctionLowCardinalityIndices(factory);
    registerFunctionLowCardinalityKeys(factory);
    registerFunctionsIn(factory);
    registerFunctionJoinGet(factory);
    registerFunctionFilesystem(factory);
    registerFunctionEvalMLMethod(factory);
    registerFunctionBasename(factory);
    registerFunctionTransform(factory);
    registerFunctionGetMacro(factory);
    registerFunctionGetScalar(factory);

#if USE_ICU
    registerFunctionConvertCharset(factory);
#endif
}

}
