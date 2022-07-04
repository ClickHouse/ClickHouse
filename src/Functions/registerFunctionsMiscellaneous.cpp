#include <config_core.h>

namespace DB
{

class FunctionFactory;

void registerFunctionCurrentDatabase(FunctionFactory &);
void registerFunctionCurrentUser(FunctionFactory &);
void registerFunctionCurrentProfiles(FunctionFactory &);
void registerFunctionCurrentRoles(FunctionFactory &);
void registerFunctionHostName(FunctionFactory &);
void registerFunctionFQDN(FunctionFactory &);
void registerFunctionVisibleWidth(FunctionFactory &);
void registerFunctionToTypeName(FunctionFactory &);
void registerFunctionGetSizeOfEnumType(FunctionFactory &);
void registerFunctionBlockSerializedSize(FunctionFactory &);
void registerFunctionToColumnTypeName(FunctionFactory &);
void registerFunctionDumpColumnStructure(FunctionFactory &);
void registerFunctionDefaultValueOfArgumentType(FunctionFactory &);
void registerFunctionDefaultValueOfTypeName(FunctionFactory &);
void registerFunctionBlockSize(FunctionFactory &);
void registerFunctionBlockNumber(FunctionFactory &);
void registerFunctionRowNumberInBlock(FunctionFactory &);
void registerFunctionRowNumberInAllBlocks(FunctionFactory &);
void registerFunctionNeighbor(FunctionFactory &);
void registerFunctionSleep(FunctionFactory &);
void registerFunctionSleepEachRow(FunctionFactory &);
void registerFunctionMaterialize(FunctionFactory &);
void registerFunctionIgnore(FunctionFactory &);
void registerFunctionIndexHint(FunctionFactory &);
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
void registerFunctionTimezone(FunctionFactory &);
void registerFunctionTimezoneOf(FunctionFactory &);
void registerFunctionRunningAccumulate(FunctionFactory &);
void registerFunctionRunningDifference(FunctionFactory &);
void registerFunctionRunningDifferenceStartingWithFirstValue(FunctionFactory &);
void registerFunctionRunningConcurrency(FunctionFactory &);
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
void registerFunctionIsDecimalOverflow(FunctionFactory &);
void registerFunctionCountDigits(FunctionFactory &);
void registerFunctionGlobalVariable(FunctionFactory &);
void registerFunctionHasThreadFuzzer(FunctionFactory &);
void registerFunctionInitializeAggregation(FunctionFactory &);
void registerFunctionErrorCodeToName(FunctionFactory &);
void registerFunctionTcpPort(FunctionFactory &);
void registerFunctionGetServerPort(FunctionFactory &);
void registerFunctionByteSize(FunctionFactory &);
void registerFunctionFile(FunctionFactory &);
void registerFunctionConnectionId(FunctionFactory &);
void registerFunctionPartitionId(FunctionFactory &);
void registerFunctionIsIPAddressContainedIn(FunctionFactory &);
void registerFunctionsTransactionCounters(FunctionFactory & factory);
void registerFunctionQueryID(FunctionFactory &);
void registerFunctionInitialQueryID(FunctionFactory &);
void registerFunctionServerUUID(FunctionFactory &);
void registerFunctionZooKeeperSessionUptime(FunctionFactory &);
void registerFunctionGetOSKernelVersion(FunctionFactory &);
void registerFunctionGetTypeSerializationStreams(FunctionFactory &);
void registerFunctionFlattenTuple(FunctionFactory &);

#if USE_ICU
void registerFunctionConvertCharset(FunctionFactory &);
#endif

#ifdef FUZZING_MODE
void registerFunctionGetFuzzerData(FunctionFactory & factory);
#endif

void registerFunctionsMiscellaneous(FunctionFactory & factory)
{
    registerFunctionCurrentDatabase(factory);
    registerFunctionCurrentUser(factory);
    registerFunctionCurrentProfiles(factory);
    registerFunctionCurrentRoles(factory);
    registerFunctionHostName(factory);
    registerFunctionFQDN(factory);
    registerFunctionVisibleWidth(factory);
    registerFunctionToTypeName(factory);
    registerFunctionGetSizeOfEnumType(factory);
    registerFunctionBlockSerializedSize(factory);
    registerFunctionToColumnTypeName(factory);
    registerFunctionDumpColumnStructure(factory);
    registerFunctionDefaultValueOfArgumentType(factory);
    registerFunctionDefaultValueOfTypeName(factory);
    registerFunctionBlockSize(factory);
    registerFunctionBlockNumber(factory);
    registerFunctionRowNumberInBlock(factory);
    registerFunctionRowNumberInAllBlocks(factory);
    registerFunctionNeighbor(factory);
    registerFunctionSleep(factory);
    registerFunctionSleepEachRow(factory);
    registerFunctionMaterialize(factory);
    registerFunctionIgnore(factory);
    registerFunctionIndexHint(factory);
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
    registerFunctionTimezone(factory);
    registerFunctionTimezoneOf(factory);
    registerFunctionRunningAccumulate(factory);
    registerFunctionRunningDifference(factory);
    registerFunctionRunningDifferenceStartingWithFirstValue(factory);
    registerFunctionRunningConcurrency(factory);
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
    registerFunctionIsDecimalOverflow(factory);
    registerFunctionCountDigits(factory);
    registerFunctionGlobalVariable(factory);
    registerFunctionHasThreadFuzzer(factory);
    registerFunctionInitializeAggregation(factory);
    registerFunctionErrorCodeToName(factory);
    registerFunctionTcpPort(factory);
    registerFunctionGetServerPort(factory);
    registerFunctionByteSize(factory);
    registerFunctionFile(factory);
    registerFunctionConnectionId(factory);
    registerFunctionPartitionId(factory);
    registerFunctionIsIPAddressContainedIn(factory);
    registerFunctionsTransactionCounters(factory);
    registerFunctionQueryID(factory);
    registerFunctionInitialQueryID(factory);
    registerFunctionServerUUID(factory);
    registerFunctionZooKeeperSessionUptime(factory);
    registerFunctionGetOSKernelVersion(factory);
    registerFunctionGetTypeSerializationStreams(factory);
    registerFunctionFlattenTuple(factory);

#if USE_ICU
    registerFunctionConvertCharset(factory);
#endif

#ifdef FUZZING_MODE
    registerFunctionGetFuzzerData(factory);
#endif
}

}
