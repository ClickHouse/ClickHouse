#if !defined(ARCADIA_BUILD)
#    include <config_core.h>
#endif

namespace DB
{

class FunctionFactory;

void registerFunctionCurrentDatabase(FunctionFactory &);
void registerFunctionCurrentUser(FunctionFactory &);
void registerFunctionHostName(FunctionFactory &);
void registerFunctionFQDN(FunctionFactory &);
void registerFunctionVisibleWidth(FunctionFactory &);
void registerFunctionToTypeName(FunctionFactory &);
void registerFunctionGetSizeOfEnumType(FunctionFactory &);
void registerFunctionBlockSerializedSize(FunctionFactory &);
void registerFunctionToColumnTypeName(FunctionFactory &);
void registerFunctionDumpColumnStructure(FunctionFactory &);
void registerFunctionDefaultValueOfArgumentType(FunctionFactory &);
void registerFunctionBlockSize(FunctionFactory &);
void registerFunctionBlockNumber(FunctionFactory &);
void registerFunctionRowNumberInBlock(FunctionFactory &);
void registerFunctionRowNumberInAllBlocks(FunctionFactory &);
void registerFunctionNeighbor(FunctionFactory &);
void registerFunctionSleep(FunctionFactory &);
void registerFunctionSleepEachRow(FunctionFactory &);
void registerFunctionMaterialize(FunctionFactory &);
void registerFunctionIgnore(FunctionFactory &);
void registerFunctionIdentity(FunctionFactory &);
void registerFunctionArrayJoin(FunctionFactory &);
void registerFunctionReplicate(FunctionFactory &);
void registerFunctionBar(FunctionFactory &);
void registerFunctionHasColumnInTable(FunctionFactory &);
void registerFunctionIsFinite(FunctionFactory &);
void registerFunctionIsInfinite(FunctionFactory &);
void registerFunctionIsNaN(FunctionFactory &);
void registerFunctionIfNotFinite(FunctionFactory &);
void registerFunctionThrowIf(FunctionFactory &);
void registerFunctionVersion(FunctionFactory &);
void registerFunctionBuildId(FunctionFactory &);
void registerFunctionUptime(FunctionFactory &);
void registerFunctionTimeZone(FunctionFactory &);
void registerFunctionRunningAccumulate(FunctionFactory &);
void registerFunctionRunningDifference(FunctionFactory &);
void registerFunctionRunningDifferenceStartingWithFirstValue(FunctionFactory &);
void registerFunctionFinalizeAggregation(FunctionFactory &);
void registerFunctionToLowCardinality(FunctionFactory &);
void registerFunctionLowCardinalityIndices(FunctionFactory &);
void registerFunctionLowCardinalityKeys(FunctionFactory &);
void registerFunctionsIn(FunctionFactory &);
void registerFunctionJoinGet(FunctionFactory &);
void registerFunctionFilesystem(FunctionFactory &);
void registerFunctionEvalMLMethod(FunctionFactory &);
void registerFunctionBasename(FunctionFactory &);
void registerFunctionTransform(FunctionFactory &);
void registerFunctionGetMacro(FunctionFactory &);
void registerFunctionGetScalar(FunctionFactory &);
void registerFunctionGetSetting(FunctionFactory &);
void registerFunctionIsConstant(FunctionFactory &);
void registerFunctionGlobalVariable(FunctionFactory &);
void registerFunctionHasThreadFuzzer(FunctionFactory &);
void registerFunctionInitializeAggregation(FunctionFactory &);

#if USE_ICU
void registerFunctionConvertCharset(FunctionFactory &);
#endif

void registerFunctionsMiscellaneous(FunctionFactory & factory)
{
    registerFunctionCurrentDatabase(factory);
    registerFunctionCurrentUser(factory);
    registerFunctionHostName(factory);
    registerFunctionFQDN(factory);
    registerFunctionVisibleWidth(factory);
    registerFunctionToTypeName(factory);
    registerFunctionGetSizeOfEnumType(factory);
    registerFunctionBlockSerializedSize(factory);
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
    registerFunctionIdentity(factory);
    registerFunctionArrayJoin(factory);
    registerFunctionReplicate(factory);
    registerFunctionBar(factory);
    registerFunctionHasColumnInTable(factory);
    registerFunctionIsFinite(factory);
    registerFunctionIsInfinite(factory);
    registerFunctionIsNaN(factory);
    registerFunctionIfNotFinite(factory);
    registerFunctionThrowIf(factory);
    registerFunctionVersion(factory);
    registerFunctionBuildId(factory);
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
    registerFunctionGetSetting(factory);
    registerFunctionIsConstant(factory);
    registerFunctionGlobalVariable(factory);
    registerFunctionHasThreadFuzzer(factory);
    registerFunctionInitializeAggregation(factory);

#if USE_ICU
    registerFunctionConvertCharset(factory);
#endif
}

}
