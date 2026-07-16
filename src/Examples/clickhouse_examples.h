#pragma once

/// Forward declarations for `clickhouse-examples` entry-point functions.
///
/// Each example translation unit defines a `mainEntryExample*` function that
/// is dispatched from `src/Examples/main.cpp`. Listing the prototypes here
/// (and including this header from each example source) ensures every
/// definition has a previous declaration, satisfying `-Wmissing-prototypes`
/// in the `arm_tidy` build.

int mainEntryExampleKerberosInit(int argc, char ** argv);
int mainEntryExampleQuantileTDigest(int argc, char ** argv);
int mainEntryExampleGroupArraySorted(int argc, char ** argv);
int mainEntryExampleTestConnect(int argc, char ** argv);
int mainEntryExampleZkutilTestCommands(int argc, char ** argv);
int mainEntryExampleZkutilTestCommandsNewLib(int argc, char ** argv);
int mainEntryExampleZkutilTestAsync(int argc, char ** argv);
int mainEntryExampleHashesTest(int argc, char ** argv);
int mainEntryExampleSipHashPerf(int argc, char ** argv);
int mainEntryExampleSmallTable(int argc, char ** argv);
int mainEntryExampleParallelAggregation(int argc, char ** argv);
int mainEntryExampleParallelAggregation2(int argc, char ** argv);
int mainEntryExampleIntHashesPerf(int argc, char ** argv);
int mainEntryExampleCompactArray(int argc, char ** argv);
int mainEntryExampleRadixSort(int argc, char ** argv);
int mainEntryExampleThreadCreationLatency(int argc, char ** argv);
int mainEntryExampleIntegerHashTablesBenchmark(int argc, char ** argv);
int mainEntryExampleCowColumns(int argc, char ** argv);
int mainEntryExampleCowCompositions(int argc, char ** argv);
int mainEntryExampleStopwatch(int argc, char ** argv);
int mainEntryExampleSymbolIndex(int argc, char ** argv);
int mainEntryExampleChaosSanitizer(int argc, char ** argv);
int mainEntryExampleMemoryStatisticsOsPerf(int argc, char ** argv);
int mainEntryExampleProcfsMetricsProviderPerf(int argc, char ** argv);
int mainEntryExampleAverage(int argc, char ** argv);
int mainEntryExampleShellCommandInout(int argc, char ** argv);
int mainEntryExampleExecutableUdf(int argc, char ** argv);
int mainEntryExampleHiveMetastoreClient(int argc, char ** argv);
int mainEntryExampleIntervalTree(int argc, char ** argv);
int mainEntryExampleEncryptDecrypt(int argc, char ** argv);
int mainEntryExampleCheckPointerValid(int argc, char ** argv);
int mainEntryExampleUtf8UpperLower(int argc, char ** argv);
int mainEntryExampleCompressedBuffer(int argc, char ** argv);
int mainEntryExampleDecompressPerf(int argc, char ** argv);
int mainEntryExampleStringPool(int argc, char ** argv);
int mainEntryExampleField(int argc, char ** argv);
int mainEntryExampleStringRefHash(int argc, char ** argv);
int mainEntryExampleGcloudStorage(int argc, char ** argv);
int mainEntryExampleGcloudKms(int argc, char ** argv);
int mainEntryExampleReadBuffer(int argc, char ** argv);
int mainEntryExampleReadBufferPerf(int argc, char ** argv);
int mainEntryExampleReadFloatPerf(int argc, char ** argv);
int mainEntryExampleWriteBuffer(int argc, char ** argv);
int mainEntryExampleWriteBufferPerf(int argc, char ** argv);
int mainEntryExampleValidUtf8Perf(int argc, char ** argv);
int mainEntryExampleValidUtf8(int argc, char ** argv);
int mainEntryExampleReadEscapedString(int argc, char ** argv);
int mainEntryExampleParseIntPerf(int argc, char ** argv);
int mainEntryExampleParseIntPerf2(int argc, char ** argv);
int mainEntryExampleReadWriteInt(int argc, char ** argv);
int mainEntryExampleODirectAndDirtyPages(int argc, char ** argv);
int mainEntryExampleIoOperators(int argc, char ** argv);
int mainEntryExampleWriteInt(int argc, char ** argv);
int mainEntryExampleZlibBuffers(int argc, char ** argv);
int mainEntryExampleLzmaBuffers(int argc, char ** argv);
int mainEntryExampleLimitReadBuffer(int argc, char ** argv);
int mainEntryExampleLimitReadBuffer2(int argc, char ** argv);
int mainEntryExampleParseDateTimeBestEffort(int argc, char ** argv);
int mainEntryExampleZlibNgBug(int argc, char ** argv);
int mainEntryExampleZstdBuffers(int argc, char ** argv);
int mainEntryExampleSnappyReadBuffer(int argc, char ** argv);
int mainEntryExampleHadoopSnappyReadBuffer(int argc, char ** argv);
int mainEntryExampleReadBufferFromHdfs(int argc, char ** argv);
int mainEntryExampleHashMap(int argc, char ** argv);
int mainEntryExampleHashMapLookup(int argc, char ** argv);
int mainEntryExampleHashMap3(int argc, char ** argv);
int mainEntryExampleHashMapString(int argc, char ** argv);
int mainEntryExampleHashMapString2(int argc, char ** argv);
int mainEntryExampleHashMapString3(int argc, char ** argv);
int mainEntryExampleHashMapStringSmall(int argc, char ** argv);
int mainEntryExampleStringHashMap(int argc, char ** argv);
int mainEntryExampleStringHashSet(int argc, char ** argv);
int mainEntryExampleTwoLevelHashMap(int argc, char ** argv);
int mainEntryExampleLexer(int argc, char ** argv);
int mainEntryExampleSelectParser(int argc, char ** argv);
int mainEntryExampleCreateParser(int argc, char ** argv);
int mainEntryExampleParserMemoryProfiler(int argc, char ** argv);
int mainEntryExampleMergeSelector(int argc, char ** argv);
int mainEntryExampleMergeSelector2(int argc, char ** argv);
int mainEntryExampleGetCurrentInsertsInReplicated(int argc, char ** argv);
