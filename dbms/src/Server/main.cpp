#include <common/config_common.h>
#include <Common/config.h>

#if USE_TCMALLOC
#include <gperftools/malloc_extension.h>
#endif

#include "Server.h"
#include "LocalServer.h"
#include <Common/StringUtils.h>

/// Universal executable for various clickhouse applications
int mainEntryClickHouseServer(int argc, char ** argv);
int mainEntryClickHouseClient(int argc, char ** argv);
int mainEntryClickHouseLocal(int argc, char ** argv);
int mainEntryClickHouseBenchmark(int argc, char ** argv);
int mainEntryClickHousePerformanceTest(int argc, char ** argv);
int mainEntryClickHouseExtractFromConfig(int argc, char ** argv);
int mainEntryClickHouseCompressor(int argc, char ** argv);
int mainEntryClickHouseFormat(int argc, char ** argv);

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
    {"local", mainEntryClickHouseLocal},
    {"client", mainEntryClickHouseClient},
    {"benchmark", mainEntryClickHouseBenchmark},
    {"server", mainEntryClickHouseServer},
    {"performance-test", mainEntryClickHousePerformanceTest},
    {"extract-from-config", mainEntryClickHouseExtractFromConfig},
    {"compressor", mainEntryClickHouseCompressor},
    {"format", mainEntryClickHouseFormat},
#if USE_EMBEDDED_COMPILER
    {"clang", mainEntryClickHouseClang},
    {"lld", mainEntryClickHouseLLD},
#endif
};


int printHelp(int, char **)
{
    std::cerr << "Use one of the following commands:" << std::endl;
    for (auto & application : clickhouse_applications)
        std::cerr << "clickhouse " << application.first << " [args] " << std::endl;
    return -1;
};


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
