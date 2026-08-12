#include <base/types.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>

#include <Parsers/ParserDataType.h>
#include <Parsers/parseQuery.h>
#include <Parsers/IAST.h>

#include <Core/Field.h>

#include <Interpreters/Context.h>

#include <AggregateFunctions/registerAggregateFunctions.h>

#include <iostream>

/// This fuzzer supports its own arguments:
/// - `-max_parser_depth=N` sets the maximum parser depth (default: 150 for debug/sanitizer, 300 otherwise)
/// - `-max_parser_backtracks=N` sets the maximum parser backtracks (default: DBMS_DEFAULT_MAX_PARSER_BACKTRACKS)
/// - `-max_query_size=N` sets the maximum query size (default: DBMS_DEFAULT_MAX_QUERY_SIZE)
/// These arguments must be passed after `-ignore_remaining_args=1` to avoid interference with libFuzzer options.

using namespace DB;

ContextMutablePtr context;

#if defined(SANITIZER) || !defined(NDEBUG)
    size_t max_parser_depth = 150;
#else
    size_t max_parser_depth = 300;
#endif
size_t max_parser_backtracks = DBMS_DEFAULT_MAX_PARSER_BACKTRACKS;
size_t max_query_size = DBMS_DEFAULT_MAX_QUERY_SIZE;

// Helper function to check if this is a merge run
bool isMerge(int argc, char ** argv)
{
    for (int i = 1; i < argc; ++i)
    {
        std::string_view arg{argv[i]};
        if (std::string_view{arg.begin(), std::ranges::find(arg, '=')} == "-ignore_remaining_args")
            break;
        if (std::string_view{arg.begin(), std::ranges::find(arg, '=')} == "-merge")
            return true;
    }
    return false;
}

// Helper function to parse settings from command line arguments
std::unordered_map<std::string, std::string> parseSettingsFromArgs(int argc, char ** argv) // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    std::unordered_map<std::string, std::string> settings; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    bool ignore_remaining = false;

    for (int i = 1; i < argc; ++i)
    {
        std::string arg{argv[i]};

        if (!ignore_remaining)
        {
            // Check for -ignore_remaining_args
            if (arg.starts_with("-ignore_remaining_args"))
            {
                ignore_remaining = true;
                continue;
            }
        }
        else
        {
            // Parse settings after -ignore_remaining_args
            size_t eq_pos = arg.find('=');
            if (eq_pos != std::string::npos)
            {
                // Skip leading dashes to get the setting name
                size_t key_start = 0;
                while (key_start < arg.length() && arg[key_start] == '-')
                    ++key_start;

                std::string key = arg.substr(key_start, eq_pos - key_start);
                std::string value = arg.substr(eq_pos + 1);
                settings[key] = value;
            }
        }
    }

    return settings;
}

extern "C" int LLVMFuzzerInitialize(const int *argc, char ***argv)
{
    if (context)
        return true;

    // Check if this is a merge run and skip initialization if so
    if (isMerge(*argc, *argv))
        return 0;

    auto settings = parseSettingsFromArgs(*argc, *argv);
    auto parse_setting = [&settings](const std::string & key, size_t & out)
    {
        if (auto it = settings.find(key); it != settings.end())
        {
            try
            {
                size_t pos = 0;
                uint64_t val = std::stoul(it->second, &pos);
                if (pos != it->second.size())
                {
                    std::cerr << "Invalid value for " << key << ": " << it->second << std::endl;
                    exit(1);
                }
                out = static_cast<size_t>(val);
            }
            catch (const std::exception & e)
            {
                std::cerr << "Invalid value for " << key << ": " << it->second << " (" << e.what() << ")" << std::endl;
                exit(1);
            }
        }
    };

    parse_setting("max_parser_depth", max_parser_depth);
    parse_setting("max_parser_backtracks", max_parser_backtracks);
    parse_setting("max_query_size", max_query_size);

    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();

    registerAggregateFunctions();
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        total_memory_tracker.resetCounters();
        total_memory_tracker.setHardLimit(1_GiB);
        CurrentThread::get().memory_tracker.resetCounters();
        CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

        /// The input format is as follows:
        /// - data type name on the first line,
        /// - the data for the rest of the input.

        /// Compile the code as follows:
        ///   mkdir build_asan_fuzz
        ///   cd build_asan_fuzz
        ///   CC=clang CXX=clang++ cmake -D SANITIZE=address -D ENABLE_FUZZING=1 -D WITH_COVERAGE=1 ..
        ///
        /// The corpus is located here:
        /// https://github.com/ClickHouse/fuzz-corpus/tree/main/data_type_deserialization
        ///
        /// The fuzzer can be run as follows:
        ///   ../../../build_asan_fuzz/src/DataTypes/fuzzers/data_type_deserialization_fuzzer corpus -jobs=64 -rss_limit_mb=8192

        /// clickhouse-local --query "SELECT toJSONString(*) FROM (SELECT name FROM system.functions UNION ALL SELECT name FROM system.data_type_families)" > dictionary

        DB::ReadBufferFromMemory in(data, size);

        String data_type;
        readStringUntilNewlineInto(data_type, in);
        assertChar('\n', in);

        DataTypePtr type;
        try
        {
            /// Parse the data type specification with depth limits
            /// This uses the same parseQuery infrastructure as other parsers with proper depth limiting
            ParserDataType parser;
            ASTPtr ast = parseQuery(parser,
                                  data_type.data(),
                                  data_type.data() + data_type.size(),
                                  "data type",
                                  max_query_size,
                                  max_parser_depth,
                                  max_parser_backtracks);

            /// Check AST depth to ensure it doesn't exceed our limits
            /// This follows the pattern used in other fuzzers
            ast->checkDepth(max_parser_depth);

            /// Now get the DataType from the parsed AST
            type = DataTypeFactory::instance().get(ast);
        }
        catch (const Exception &)
        {
            /// If parsing fails due to depth limits or other parsing errors,
            /// this is the expected behavior - ClickHouse will throw proper exceptions
            return 0;
        }

        FormatSettings settings;
        settings.binary.max_binary_array_size = 100;
        settings.binary.max_binary_string_size = 100;

        Field field;
        type->getDefaultSerialization()->deserializeBinary(field, in, settings);
    }
    catch (...)
    {
        // Ok
    }

    return 0;
}
