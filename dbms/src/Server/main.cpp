#include <common/config_common.h>
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
int mainEntryClickHouseExtractFromConfig(int argc, char ** argv);


static bool isClickhouseApp(const std::string & app_suffix, std::vector<char *> & argv)
{
    std::string arg_mode_app = "--" + app_suffix;

    /// Use app if --app arg is passed (the arg should be quietly removed)
    auto arg_it = std::find_if(argv.begin(), argv.end(), [&](const char * arg) { return !arg_mode_app.compare(arg); } );
    if (arg_it != argv.end())
    {
        argv.erase(arg_it);
        return true;
    }

    std::string app_name = "clickhouse-" + app_suffix;

    /// Use app if clickhouse binary is run through symbolic link with name clickhouse-app
    if (!argv.empty() && (!app_name.compare(argv[0]) || endsWith(argv[0], "/" + app_name)))
        return true;

    return false;
}


int main(int argc_, char ** argv_)
{
#if USE_TCMALLOC
    MallocExtension::instance()->SetNumericProperty("tcmalloc.aggressive_memory_decommit", false);
#endif

    std::vector<char *> argv(argv_, argv_ + argc_);

    auto main_func = mainEntryClickHouseServer;

    if (isClickhouseApp("local", argv))
        main_func = mainEntryClickHouseLocal;
    else if (isClickhouseApp("client", argv))
        main_func = mainEntryClickHouseClient;
    else if (isClickhouseApp("benchmark", argv))
        main_func = mainEntryClickHouseBenchmark;
    else if (isClickhouseApp("server", argv)) /// --server arg should be cut
        main_func = mainEntryClickHouseServer;
    else if (isClickhouseApp("extract-from-config", argv))
        main_func = mainEntryClickHouseExtractFromConfig;

    return main_func(static_cast<int>(argv.size()), argv.data());
}
