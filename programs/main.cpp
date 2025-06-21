#include <unistd.h>
#include <fcntl.h>

#include <new>
#include <iostream>
#include <vector>
#include <string>
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

#include <base/phdr_cache.h>
#include <base/coverage.h>


/// Universal executable for various clickhouse applications
int mainEntryClickHouseServer(int argc, char ** argv);
int mainEntryClickHouseClient(int argc, char ** argv);
int mainEntryClickHouseLocal(int argc, char ** argv);
int mainEntryClickHouseBenchmark(int argc, char ** argv);
int mainEntryClickHouseExtractFromConfig(int argc, char ** argv);
int mainEntryClickHouseCompressor(int argc, char ** argv);
int mainEntryClickHouseFormat(int argc, char ** argv);
int mainEntryClickHouseObfuscator(int argc, char ** argv);
int mainEntryClickHouseGitImport(int argc, char ** argv);
int mainEntryClickHouseStaticFilesDiskUploader(int argc, char ** argv);
int mainEntryClickHouseSU(int argc, char ** argv);
int mainEntryClickHouseDisks(int argc, char ** argv);

int mainEntryClickHouseHashBinary(int, char **)
{
    /// Intentionally without newline. So you can run:
    /// objcopy --add-section .clickhouse.hash=<(./clickhouse hash-binary) clickhouse
    std::cout << getHashOfLoadedBinaryHex();
    return 0;
}

#if ENABLE_CLICKHOUSE_KEEPER
int mainEntryClickHouseKeeper(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_KEEPER_CONVERTER
int mainEntryClickHouseKeeperConverter(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_KEEPER_CLIENT
int mainEntryClickHouseKeeperClient(int argc, char ** argv);
#endif

// install
int mainEntryClickHouseInstall(int argc, char ** argv);
int mainEntryClickHouseStart(int argc, char ** argv);
int mainEntryClickHouseStop(int argc, char ** argv);
int mainEntryClickHouseStatus(int argc, char ** argv);
int mainEntryClickHouseRestart(int argc, char ** argv);

namespace
{

using MainFunc = int (*)(int, char**);

/// Add an item here to register new application
std::pair<std::string_view, MainFunc> clickhouse_applications[] =
{
    {"local", mainEntryClickHouseLocal},
    {"client", mainEntryClickHouseClient},
    {"benchmark", mainEntryClickHouseBenchmark},
    {"server", mainEntryClickHouseServer},
    {"extract-from-config", mainEntryClickHouseExtractFromConfig},
    {"compressor", mainEntryClickHouseCompressor},
    {"format", mainEntryClickHouseFormat},
    {"obfuscator", mainEntryClickHouseObfuscator},
    {"git-import", mainEntryClickHouseGitImport},
    {"static-files-disk-uploader", mainEntryClickHouseStaticFilesDiskUploader},
    {"su", mainEntryClickHouseSU},
    {"hash-binary", mainEntryClickHouseHashBinary},
    {"disks", mainEntryClickHouseDisks},

    // keeper
#if ENABLE_CLICKHOUSE_KEEPER
    {"keeper", mainEntryClickHouseKeeper},
#endif
#if ENABLE_CLICKHOUSE_KEEPER_CONVERTER
    {"keeper-converter", mainEntryClickHouseKeeperConverter},
#endif
#if ENABLE_CLICKHOUSE_KEEPER_CLIENT
    {"keeper-client", mainEntryClickHouseKeeperClient},
#endif

    // install
    {"install", mainEntryClickHouseInstall},
    {"start", mainEntryClickHouseStart},
    {"stop", mainEntryClickHouseStop},
    {"status", mainEntryClickHouseStatus},
    {"restart", mainEntryClickHouseRestart},
};

int printHelp(int, char **)
{
    std::cerr << "Use one of the following commands:" << std::endl;
    for (auto & application : clickhouse_applications)
        std::cerr << "clickhouse " << application.first << " [args] " << std::endl;
    return -1;
}

/// Add an item here to register a new short name
std::pair<std::string_view, std::string_view> clickhouse_short_names[] =
{
    {"chl", "local"},
    {"chc", "client"},
};

}

bool isClickhouseApp(std::string_view app_suffix, std::vector<char *> & argv)
{
    for (const auto & [alias, name] : clickhouse_short_names)
        if (app_suffix == name
            && !argv.empty() && (alias == argv[0] || endsWith(argv[0], "/" + std::string(alias))))
            return true;

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
bool inside_main = false;

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
    MainFunc main_func = printHelp;

    for (auto & application : clickhouse_applications)
    {
        if (isClickhouseApp(application.first, argv))
        {
            main_func = application.second;
            break;
        }
    }

    /// Interpret binary without argument or with arguments starts with dash
    /// ('-') as clickhouse-local for better usability:
    ///
    ///     clickhouse help # dumps help
    ///     clickhouse -q 'select 1' # use local
    ///     clickhouse # spawn local
    ///     clickhouse local # spawn local
    ///     clickhouse "select ..." # spawn local
    ///
    if (main_func == printHelp && !argv.empty() && (argv.size() == 1 || argv[1][0] == '-'
        || std::string_view(argv[1]).contains(' ')))
    {
        main_func = mainEntryClickHouseLocal;
    }

    int exit_code = main_func(static_cast<int>(argv.size()), argv.data());

#if defined(SANITIZE_COVERAGE)
    dumpCoverage();
#endif

    return exit_code;
}
