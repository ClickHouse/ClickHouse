#include "config.h"

#include <iostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <filesystem>

#if USE_KRB5
int mainEntryExampleKerberosInit(int argc, char ** argv);
#endif
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
#if defined(OS_LINUX)
int mainEntryExampleThreadCreationLatency(int argc, char ** argv);
#endif
int mainEntryExampleIntegerHashTablesBenchmark(int argc, char ** argv);
int mainEntryExampleCowColumns(int argc, char ** argv);
int mainEntryExampleCowCompositions(int argc, char ** argv);
int mainEntryExampleStopwatch(int argc, char ** argv);
int mainEntryExampleSymbolIndex(int argc, char ** argv);
int mainEntryExampleChaosSanitizer(int argc, char ** argv);
#if defined(OS_LINUX)
int mainEntryExampleMemoryStatisticsOsPerf(int argc, char ** argv);
#endif
int mainEntryExampleProcfsMetricsProviderPerf(int argc, char ** argv);
int mainEntryExampleAverage(int argc, char ** argv);
int mainEntryExampleShellCommandInout(int argc, char ** argv);
int mainEntryExampleExecutableUdf(int argc, char ** argv);
#if USE_HIVE
int mainEntryExampleHiveMetastoreClient(int argc, char ** argv);
#endif
int mainEntryExampleIntervalTree(int argc, char ** argv);
#if USE_SSL
int mainEntryExampleEncryptDecrypt(int argc, char ** argv);
#endif
int mainEntryExampleCheckPointerValid(int argc, char ** argv);
#if USE_ICU
int mainEntryExampleUtf8UpperLower(int argc, char ** argv);
#endif
int mainEntryExampleCompressedBuffer(int argc, char ** argv);
int mainEntryExampleDecompressPerf(int argc, char ** argv);
int mainEntryExampleStringPool(int argc, char ** argv);
int mainEntryExampleField(int argc, char ** argv);
int mainEntryExampleStringRefHash(int argc, char ** argv);
#if USE_GOOGLE_CLOUD
int mainEntryExampleGcloudStorage(int argc, char ** argv);
#endif
#if USE_GOOGLE_CLOUD_KMS
int mainEntryExampleGcloudKms(int argc, char ** argv);
#endif
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
#if USE_HDFS
int mainEntryExampleReadBufferFromHdfs(int argc, char ** argv);
#endif
int mainEntryExampleHashMap(int argc, char ** argv);
int mainEntryExampleHashMapLookup(int argc, char ** argv);
int mainEntryExampleHashMap3(int argc, char ** argv);
int mainEntryExampleHashMapString(int argc, char ** argv);
int mainEntryExampleHashMapString2(int argc, char ** argv);
int mainEntryExampleHashMapString3(int argc, char ** argv);
int mainEntryExampleHashMapStringSmall(int argc, char ** argv);
int mainEntryExampleStringHashMap(int argc, char ** argv);
/// string_hash_map_aggregation uses the same source as string_hash_map
int mainEntryExampleStringHashSet(int argc, char ** argv);
int mainEntryExampleTwoLevelHashMap(int argc, char ** argv);
int mainEntryExampleLexer(int argc, char ** argv);
int mainEntryExampleSelectParser(int argc, char ** argv);
int mainEntryExampleCreateParser(int argc, char ** argv);
int mainEntryExampleParserMemoryProfiler(int argc, char ** argv);
int mainEntryExampleMergeSelector(int argc, char ** argv);
int mainEntryExampleMergeSelector2(int argc, char ** argv);
int mainEntryExampleGetCurrentInsertsInReplicated(int argc, char ** argv);

namespace
{

using MainFunc = int (*)(int, char **);

std::pair<std::string_view, MainFunc> examples[] =
{
#if USE_KRB5
    {"kerberos_init", mainEntryExampleKerberosInit},
#endif
    {"quantile-t-digest", mainEntryExampleQuantileTDigest},
    {"group_array_sorted", mainEntryExampleGroupArraySorted},
    {"test-connect", mainEntryExampleTestConnect},
    {"zkutil_test_commands", mainEntryExampleZkutilTestCommands},
    {"zkutil_test_commands_new_lib", mainEntryExampleZkutilTestCommandsNewLib},
    {"zkutil_test_async", mainEntryExampleZkutilTestAsync},
    {"hashes_test", mainEntryExampleHashesTest},
    {"sip_hash_perf", mainEntryExampleSipHashPerf},
    {"small_table", mainEntryExampleSmallTable},
    {"parallel_aggregation", mainEntryExampleParallelAggregation},
    {"parallel_aggregation2", mainEntryExampleParallelAggregation2},
    {"int_hashes_perf", mainEntryExampleIntHashesPerf},
    {"compact_array", mainEntryExampleCompactArray},
    {"radix_sort", mainEntryExampleRadixSort},
#if defined(OS_LINUX)
    {"thread_creation_latency", mainEntryExampleThreadCreationLatency},
#endif
    {"integer_hash_tables_benchmark", mainEntryExampleIntegerHashTablesBenchmark},
    {"cow_columns", mainEntryExampleCowColumns},
    {"cow_compositions", mainEntryExampleCowCompositions},
    {"stopwatch", mainEntryExampleStopwatch},
    {"symbol_index", mainEntryExampleSymbolIndex},
    {"chaos_sanitizer", mainEntryExampleChaosSanitizer},
#if defined(OS_LINUX)
    {"memory_statistics_os_perf", mainEntryExampleMemoryStatisticsOsPerf},
#endif
    {"procfs_metrics_provider_perf", mainEntryExampleProcfsMetricsProviderPerf},
    {"average", mainEntryExampleAverage},
    {"shell_command_inout", mainEntryExampleShellCommandInout},
    {"executable_udf", mainEntryExampleExecutableUdf},
#if USE_HIVE
    {"hive_metastore_client", mainEntryExampleHiveMetastoreClient},
#endif
    {"interval_tree", mainEntryExampleIntervalTree},
#if USE_SSL
    {"encrypt_decrypt", mainEntryExampleEncryptDecrypt},
#endif
    {"check_pointer_valid", mainEntryExampleCheckPointerValid},
#if USE_ICU
    {"utf8_upper_lower", mainEntryExampleUtf8UpperLower},
#endif
    {"compressed_buffer", mainEntryExampleCompressedBuffer},
    {"decompress_perf", mainEntryExampleDecompressPerf},
    {"string_pool", mainEntryExampleStringPool},
    {"field", mainEntryExampleField},
    {"string_ref_hash", mainEntryExampleStringRefHash},
#if USE_GOOGLE_CLOUD
    {"gcloud_storage", mainEntryExampleGcloudStorage},
#endif
#if USE_GOOGLE_CLOUD_KMS
    {"gcloud_kms", mainEntryExampleGcloudKms},
#endif
    {"read_buffer", mainEntryExampleReadBuffer},
    {"read_buffer_perf", mainEntryExampleReadBufferPerf},
    {"read_float_perf", mainEntryExampleReadFloatPerf},
    {"write_buffer", mainEntryExampleWriteBuffer},
    {"write_buffer_perf", mainEntryExampleWriteBufferPerf},
    {"valid_utf8_perf", mainEntryExampleValidUtf8Perf},
    {"valid_utf8", mainEntryExampleValidUtf8},
    {"read_escaped_string", mainEntryExampleReadEscapedString},
    {"parse_int_perf", mainEntryExampleParseIntPerf},
    {"parse_int_perf2", mainEntryExampleParseIntPerf2},
    {"read_write_int", mainEntryExampleReadWriteInt},
    {"o_direct_and_dirty_pages", mainEntryExampleODirectAndDirtyPages},
    {"io_operators", mainEntryExampleIoOperators},
    {"write_int", mainEntryExampleWriteInt},
    {"zlib_buffers", mainEntryExampleZlibBuffers},
    {"lzma_buffers", mainEntryExampleLzmaBuffers},
    {"limit_read_buffer", mainEntryExampleLimitReadBuffer},
    {"limit_read_buffer2", mainEntryExampleLimitReadBuffer2},
    {"parse_date_time_best_effort", mainEntryExampleParseDateTimeBestEffort},
    {"zlib_ng_bug", mainEntryExampleZlibNgBug},
    {"zstd_buffers", mainEntryExampleZstdBuffers},
    {"snappy_read_buffer", mainEntryExampleSnappyReadBuffer},
    {"hadoop_snappy_read_buffer", mainEntryExampleHadoopSnappyReadBuffer},
#if USE_HDFS
    {"read_buffer_from_hdfs", mainEntryExampleReadBufferFromHdfs},
#endif
    {"hash_map", mainEntryExampleHashMap},
    {"hash_map_lookup", mainEntryExampleHashMapLookup},
    {"hash_map3", mainEntryExampleHashMap3},
    {"hash_map_string", mainEntryExampleHashMapString},
    {"hash_map_string_2", mainEntryExampleHashMapString2},
    {"hash_map_string_3", mainEntryExampleHashMapString3},
    {"hash_map_string_small", mainEntryExampleHashMapStringSmall},
    {"string_hash_map", mainEntryExampleStringHashMap},
    {"string_hash_map_aggregation", mainEntryExampleStringHashMap},
    {"string_hash_set", mainEntryExampleStringHashSet},
    {"two_level_hash_map", mainEntryExampleTwoLevelHashMap},
    {"lexer", mainEntryExampleLexer},
    {"select_parser", mainEntryExampleSelectParser},
    {"create_parser", mainEntryExampleCreateParser},
    {"parser_memory_profiler", mainEntryExampleParserMemoryProfiler},
    {"merge_selector", mainEntryExampleMergeSelector},
    {"merge_selector2", mainEntryExampleMergeSelector2},
    {"get_current_inserts_in_replicated", mainEntryExampleGetCurrentInsertsInReplicated},
};

void printHelp()
{
    std::cerr << "Usage: clickhouse-examples <example-name> [args]" << std::endl;
    std::cerr << std::endl;
    std::cerr << "Available examples:" << std::endl;
    for (const auto & example : examples)
        std::cerr << "  " << example.first << std::endl;
}

}

int main(int argc, char ** argv)
{
    std::vector<char *> args(argv, argv + argc);

    if (args.empty())
    {
        printHelp();
        return 1;
    }

    /// Check if invoked via symlink (e.g., hash_map -> clickhouse-examples)
    std::string binary_name = std::filesystem::path(args[0]).filename().string();

    for (const auto & [name, func] : examples)
    {
        if (binary_name == name)
            return func(argc, argv);
    }

    /// Handle --help / -h explicitly with exit code 0
    if (args.size() >= 2)
    {
        std::string_view first_arg(args[1]);
        if (first_arg == "--help" || first_arg == "-h")
        {
            printHelp();
            return 0;
        }
    }

    /// Check first argument as subcommand
    if (args.size() >= 2)
    {
        std::string_view command(args[1]);
        for (const auto & [name, func] : examples)
        {
            if (command == name)
            {
                /// Rewrite argv[0] to the matched command name so examples
                /// that derive usage text from program name show correct output.
                args[0] = args[1];
                /// Remove the subcommand from args
                args.erase(args.begin() + 1);
                return func(static_cast<int>(args.size()), args.data());
            }
        }
    }

    printHelp();
    return 1;
}
