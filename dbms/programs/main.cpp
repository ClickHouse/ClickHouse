#include <new>
#include <iostream>
#include <vector>
#include <string>
#include <utility> /// pair

#if __has_include("config_tools.h")
#include "config_tools.h"
#endif
#if __has_include(<common/config_common.h>)     /// "Arcadia" build system lacks configure files.
#include <common/config_common.h>
#endif
#if __has_include(<Common/config.h>)
#include <Common/config.h>
#endif

#if USE_TCMALLOC
#include <gperftools/malloc_extension.h> // Y_IGNORE
#endif

#include <Common/StringUtils/StringUtils.h>

/// Universal executable for various clickhouse applications
#if ENABLE_CLICKHOUSE_SERVER || !defined(ENABLE_CLICKHOUSE_SERVER)
int mainEntryClickHouseServer(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_CLIENT || !defined(ENABLE_CLICKHOUSE_CLIENT)
int mainEntryClickHouseClient(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_LOCAL || !defined(ENABLE_CLICKHOUSE_LOCAL)
int mainEntryClickHouseLocal(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_BENCHMARK || !defined(ENABLE_CLICKHOUSE_BENCHMARK)
int mainEntryClickHouseBenchmark(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_PERFORMANCE_TEST || !defined(ENABLE_CLICKHOUSE_PERFORMANCE_TEST)
int mainEntryClickHousePerformanceTest(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG || !defined(ENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG)
int mainEntryClickHouseExtractFromConfig(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_COMPRESSOR || !defined(ENABLE_CLICKHOUSE_COMPRESSOR)
int mainEntryClickHouseCompressor(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_FORMAT || !defined(ENABLE_CLICKHOUSE_FORMAT)
int mainEntryClickHouseFormat(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_COPIER || !defined(ENABLE_CLICKHOUSE_COPIER)
int mainEntryClickHouseClusterCopier(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_OBFUSCATOR || !defined(ENABLE_CLICKHOUSE_OBFUSCATOR)
int mainEntryClickHouseObfuscator(int argc, char ** argv);
#endif


#if USE_EMBEDDED_COMPILER
    int mainEntryClickHouseClang(int argc, char ** argv);
    int mainEntryClickHouseLLD(int argc, char ** argv);
#endif

namespace
{

using MainFunc = int (*)(int, char**);


/// Add an item here to register new application
std::pair<const char *, MainFunc> clickhouse_applications[] =
{
#if ENABLE_CLICKHOUSE_LOCAL || !defined(ENABLE_CLICKHOUSE_LOCAL)
    {"local", mainEntryClickHouseLocal},
#endif
#if ENABLE_CLICKHOUSE_CLIENT || !defined(ENABLE_CLICKHOUSE_CLIENT)
    {"client", mainEntryClickHouseClient},
#endif
#if ENABLE_CLICKHOUSE_BENCHMARK || !defined(ENABLE_CLICKHOUSE_BENCHMARK)
    {"benchmark", mainEntryClickHouseBenchmark},
#endif
#if ENABLE_CLICKHOUSE_SERVER || !defined(ENABLE_CLICKHOUSE_SERVER)
    {"server", mainEntryClickHouseServer},
#endif
#if ENABLE_CLICKHOUSE_PERFORMANCE_TEST || !defined(ENABLE_CLICKHOUSE_PERFORMANCE_TEST)
    {"performance-test", mainEntryClickHousePerformanceTest},
#endif
#if ENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG || !defined(ENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG)
    {"extract-from-config", mainEntryClickHouseExtractFromConfig},
#endif
#if ENABLE_CLICKHOUSE_COMPRESSOR || !defined(ENABLE_CLICKHOUSE_COMPRESSOR)
    {"compressor", mainEntryClickHouseCompressor},
#endif
#if ENABLE_CLICKHOUSE_FORMAT || !defined(ENABLE_CLICKHOUSE_FORMAT)
    {"format", mainEntryClickHouseFormat},
#endif
#if ENABLE_CLICKHOUSE_COPIER || !defined(ENABLE_CLICKHOUSE_COPIER)
    {"copier", mainEntryClickHouseClusterCopier},
#endif
#if ENABLE_CLICKHOUSE_OBFUSCATOR || !defined(ENABLE_CLICKHOUSE_OBFUSCATOR)
    {"obfuscator", mainEntryClickHouseObfuscator},
#endif

#if USE_EMBEDDED_COMPILER
    {"clang", mainEntryClickHouseClang},
    {"clang++", mainEntryClickHouseClang},
    {"lld", mainEntryClickHouseLLD},
#endif
};


int printHelp(int, char **)
{
    std::cerr << "Use one of the following commands:" << std::endl;
    for (auto & application : clickhouse_applications)
        std::cerr << "clickhouse " << application.first << " [args] " << std::endl;
    return -1;
}


bool isClickhouseApp(const std::string & app_suffix, std::vector<char *> & argv)
{
    /// Use app if the first arg 'app' is passed (the arg should be quietly removed)
    if (argv.size() >= 2)
    {
        auto first_arg = argv.begin() + 1;

        /// 'clickhouse --client ...' and 'clickhouse client ...' are Ok
        if (*first_arg == "--" + app_suffix || *first_arg == app_suffix)
        {
            argv.erase(first_arg);
            return true;
        }
    }

    /// Use app if clickhouse binary is run through symbolic link with name clickhouse-app
    std::string app_name = "clickhouse-" + app_suffix;
    return !argv.empty() && (app_name == argv[0] || endsWith(argv[0], "/" + app_name));
}

}


int main(int argc_, char ** argv_)
{
    /// Reset new handler to default (that throws std::bad_alloc)
    /// It is needed because LLVM library clobbers it.
    std::set_new_handler(nullptr);

#if USE_EMBEDDED_COMPILER
    if (argc_ >= 2 && 0 == strcmp(argv_[1], "-cc1"))
        return mainEntryClickHouseClang(argc_, argv_);
#endif

#if USE_TCMALLOC
    /** Without this option, tcmalloc returns memory to OS too frequently for medium-sized memory allocations
      *  (like IO buffers, column vectors, hash tables, etc.),
      *  that lead to page faults and significantly hurts performance.
      */
    MallocExtension::instance()->SetNumericProperty("tcmalloc.aggressive_memory_decommit", false);
#endif

    std::vector<char *> argv(argv_, argv_ + argc_);

    /// Print a basic help if nothing was matched
    MainFunc main_func = printHelp;

    for (auto & application : clickhouse_applications)
    {
        if (isClickhouseApp(application.first, argv))
        {
            main_func = application.second;
            break;
        }
    }

    return main_func(static_cast<int>(argv.size()), argv.data());
}
