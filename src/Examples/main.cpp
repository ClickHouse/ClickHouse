#include "config.h"

#include <Examples/clickhouse_examples.h>

#include <iostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <filesystem>

#include <Common/VectorWithMemoryTracking.h>

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
    DB::VectorWithMemoryTracking<char *> args(argv, argv + argc);

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
