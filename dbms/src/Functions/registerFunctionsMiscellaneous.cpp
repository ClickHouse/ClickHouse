namespace DB
{

class FunctionFactory;

void registerFunctionCurrentDatabase(FunctionFactory &);
void registerFunctionHostName(FunctionFactory &);
void registerFunctionVisibleWidth(FunctionFactory &);
void registerFunctionToTypeName(FunctionFactory &);
void registerFunctionGetSizeOfEnumType(FunctionFactory &);
void registerFunctionToColumnTypeName(FunctionFactory &);
void registerFunctionDumpColumnStructure(FunctionFactory &);
void registerFunctionDefaultValueOfArgumentType(FunctionFactory &);
void registerFunctionBlockSize(FunctionFactory &);
void registerFunctionBlockNumber(FunctionFactory &);
void registerFunctionRowNumberInBlock(FunctionFactory &);
void registerFunctionRowNumberInAllBlocks(FunctionFactory &);
void registerFunctionSleep(FunctionFactory &);
void registerFunctionSleepEachRow(FunctionFactory &);
void registerFunctionMaterialize(FunctionFactory &);
void registerFunctionIgnore(FunctionFactory &);
void registerFunctionIgnoreExceptNull(FunctionFactory &);
void registerFunctionIndexHint(FunctionFactory &);
void registerFunctionIdentity(FunctionFactory &);
void registerFunctionArrayJoin(FunctionFactory &);
void registerFunctionReplicate(FunctionFactory &);
void registerFunctionBar(FunctionFactory &);
void registerFunctionHasColumnInTable(FunctionFactory &);
void registerFunctionIsFinite(FunctionFactory &);
void registerFunctionIsInfinite(FunctionFactory &);
void registerFunctionIsNaN(FunctionFactory &);
void registerFunctionThrowIf(FunctionFactory &);
void registerFunctionVersion(FunctionFactory &);
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

void registerFunctionsMiscellaneous(FunctionFactory & factory)
{
    registerFunctionCurrentDatabase(factory);
    registerFunctionHostName(factory);
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
}

}
