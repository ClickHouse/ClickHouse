#include <unistd.h>
#include <fcntl.h>

#include <new>
#include <iostream>
#include <vector>
#include <string_view>
#include <utility> /// pair

#include <fmt/format.h>

#include "config.h"
#include "config_tools.h"

#include <Common/EnvironmentChecks.h>
#include <Common/Coverage.h>

#include <Common/StringUtils.h>
#include <Common/getHashOfLoadedBinary.h>
#include <Common/IO.h>

#include <base/coverage.h>
#include <base/phdr_cache.h>
#include <base/scope_guard.h>


int mainEntryClickHouseKeeper(int argc, char ** argv);
#if ENABLE_CLICKHOUSE_KEEPER_CONVERTER
int mainEntryClickHouseKeeperConverter(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_KEEPER_CLIENT
int mainEntryClickHouseKeeperClient(int argc, char ** argv);
#endif

namespace
{

using MainFunc = int (*)(int, char**);

/// Add an item here to register new application
std::pair<std::string_view, MainFunc> clickhouse_applications[] =
{
    // keeper
    {"keeper", mainEntryClickHouseKeeper},
#if ENABLE_CLICKHOUSE_KEEPER_CONVERTER
    {"converter", mainEntryClickHouseKeeperConverter},
    {"keeper-converter", mainEntryClickHouseKeeperConverter},
#endif
#if ENABLE_CLICKHOUSE_KEEPER_CLIENT
    {"client", mainEntryClickHouseKeeperClient},
    {"keeper-client", mainEntryClickHouseKeeperClient},
#endif

};

int printHelp(int, char **)
{
    std::cerr << "Use one of the following commands:" << std::endl;
    for (auto & application : clickhouse_applications)
        std::cerr << "clickhouse " << application.first << " [args] " << std::endl;
    return -1;
}

}


static bool isClickhouseApp(std::string_view app_suffix, std::vector<char *> & argv)
{
    /// Use app if the first arg 'app' is passed (the arg should be quietly removed)
    if (argv.size() >= 2)
    {
        auto first_arg = argv.begin() + 1;

        /// 'clickhouse --client ...' and 'clickhouse client ...' are Ok
        if (*first_arg == app_suffix
            || (std::string_view(*first_arg).starts_with("--") && std::string_view(*first_arg).substr(2) == app_suffix))
        {
            argv.erase(first_arg);
            return true;
        }
    }

    /// keeper suffix is default which will be used if no other app is detected
    if (app_suffix == "keeper")
        return false;

    /// Use app if clickhouse binary is run through symbolic link with name clickhouse-app
    std::string app_name = "clickhouse-" + std::string(app_suffix);
    return !argv.empty() && (app_name == argv[0] || endsWith(argv[0], "/" + app_name));
}

/// Don't allow dlopen in the main ClickHouse binary, because it is harmful and insecure.
/// We don't use it. But it can be used by some libraries for implementation of "plugins".
/// We absolutely discourage the ancient technique of loading
/// 3rd-party uncontrolled dangerous libraries into the process address space,
/// because it is insane.

#if !defined(USE_MUSL)
extern "C"
{
    void * dlopen(const char *, int)
    {
        return nullptr;
    }

    void * dlmopen(long, const char *, int) // NOLINT
    {
        return nullptr;
    }

    int dlclose(void *)
    {
        return 0;
    }

    const char * dlerror()
    {
        return "ClickHouse does not allow dynamic library loading";
    }
}
#endif

/// Prevent messages from JeMalloc in the release build.
/// Some of these messages are non-actionable for the users, such as:
/// <jemalloc>: Number of CPUs detected is not deterministic. Per-CPU arena disabled.
#if USE_JEMALLOC && defined(NDEBUG) && !defined(SANITIZER)
extern "C" void (*malloc_message)(void *, const char *s);
__attribute__((constructor(0))) void init_je_malloc_message() { malloc_message = [](void *, const char *){}; }
#endif

/// This allows to implement assert to forbid initialization of a class in static constructors.
/// Usage:
///
/// extern bool inside_main;
/// class C { C() { assert(inside_main); } };
static bool inside_main = false;

int main(int argc_, char ** argv_)
{
    inside_main = true;
    SCOPE_EXIT({ inside_main = false; });

    /// PHDR cache is required for query profiler to work reliably
    /// It also speed up exception handling, but exceptions from dynamically loaded libraries (dlopen)
    ///  will work only after additional call of this function.
    /// Note: we forbid dlopen in our code.
    updatePHDRCache();

#if !defined(USE_MUSL)
    checkHarmfulEnvironmentVariables(argv_);
#endif

    /// This is used for testing. For example,
    /// clickhouse-local should be able to run a simple query without throw/catch.
    if (getenv("CLICKHOUSE_TERMINATE_ON_ANY_EXCEPTION")) // NOLINT(concurrency-mt-unsafe)
        DB::terminate_on_any_exception = true;

    /// Reset new handler to default (that throws std::bad_alloc)
    /// It is needed because LLVM library clobbers it.
    std::set_new_handler(nullptr);

    std::vector<char *> argv(argv_, argv_ + argc_);

    /// Print a basic help if nothing was matched
    MainFunc main_func = mainEntryClickHouseKeeper;

    if (isClickhouseApp("help", argv))
    {
        main_func = printHelp;
    }
    else
    {
        for (auto & application : clickhouse_applications)
        {
            if (isClickhouseApp(application.first, argv))
            {
                main_func = application.second;
                break;
            }
        }
    }

    int exit_code = main_func(static_cast<int>(argv.size()), argv.data());

#if defined(SANITIZE_COVERAGE)
    dumpCoverage();
#endif

    return exit_code;
}
