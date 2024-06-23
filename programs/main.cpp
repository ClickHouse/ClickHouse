#include <csignal>
#include <csetjmp>
#include <unistd.h>
#include <fcntl.h>

#include <new>
#include <iostream>
#include <vector>
#include <string>
#include <tuple>
#include <string_view>
#include <utility> /// pair

#include <fmt/format.h>

#include "config_tools.h"

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


enum class InstructionFail : uint8_t
{
    NONE = 0,
    SSE3 = 1,
    SSSE3 = 2,
    SSE4_1 = 3,
    SSE4_2 = 4,
    POPCNT = 5,
    AVX = 6,
    AVX2 = 7,
    AVX512 = 8
};

auto instructionFailToString(InstructionFail fail)
{
    switch (fail)
    {
#define ret(x) return std::make_tuple(STDERR_FILENO, x, sizeof(x) - 1)
        case InstructionFail::NONE:
            ret("NONE");
        case InstructionFail::SSE3:
            ret("SSE3");
        case InstructionFail::SSSE3:
            ret("SSSE3");
        case InstructionFail::SSE4_1:
            ret("SSE4.1");
        case InstructionFail::SSE4_2:
            ret("SSE4.2");
        case InstructionFail::POPCNT:
            ret("POPCNT");
        case InstructionFail::AVX:
            ret("AVX");
        case InstructionFail::AVX2:
            ret("AVX2");
        case InstructionFail::AVX512:
            ret("AVX512");
#undef ret
    }
}


sigjmp_buf jmpbuf;

[[noreturn]] void sigIllCheckHandler(int, siginfo_t *, void *)
{
    siglongjmp(jmpbuf, 1);
}

/// Check if necessary SSE extensions are available by trying to execute some sse instructions.
/// If instruction is unavailable, SIGILL will be sent by kernel.
void checkRequiredInstructionsImpl(volatile InstructionFail & fail)
{
#if defined(__SSE3__)
    fail = InstructionFail::SSE3;
    __asm__ volatile ("addsubpd %%xmm0, %%xmm0" : : : "xmm0");
#endif

#if defined(__SSSE3__)
    fail = InstructionFail::SSSE3;
    __asm__ volatile ("pabsw %%xmm0, %%xmm0" : : : "xmm0");

#endif

#if defined(__SSE4_1__)
    fail = InstructionFail::SSE4_1;
    __asm__ volatile ("pmaxud %%xmm0, %%xmm0" : : : "xmm0");
#endif

#if defined(__SSE4_2__)
    fail = InstructionFail::SSE4_2;
    __asm__ volatile ("pcmpgtq %%xmm0, %%xmm0" : : : "xmm0");
#endif

    /// Defined by -msse4.2
#if defined(__POPCNT__)
    fail = InstructionFail::POPCNT;
    {
        uint64_t a = 0;
        uint64_t b = 0;
        __asm__ volatile ("popcnt %1, %0" : "=r"(a) :"r"(b) :);
    }
#endif

#if defined(__AVX__)
    fail = InstructionFail::AVX;
    __asm__ volatile ("vaddpd %%ymm0, %%ymm0, %%ymm0" : : : "ymm0");
#endif

#if defined(__AVX2__)
    fail = InstructionFail::AVX2;
    __asm__ volatile ("vpabsw %%ymm0, %%ymm0" : : : "ymm0");
#endif

#if defined(__AVX512__)
    fail = InstructionFail::AVX512;
    __asm__ volatile ("vpabsw %%zmm0, %%zmm0" : : : "zmm0");
#endif

    fail = InstructionFail::NONE;
}

/// Macros to avoid using strlen(), since it may fail if SSE is not supported.
#define writeError(data) do \
    { \
        static_assert(__builtin_constant_p(data)); \
        if (!writeRetry(STDERR_FILENO, data, sizeof(data) - 1)) \
            _Exit(1); \
    } while (false)

/// Check SSE and others instructions availability. Calls exit on fail.
/// This function must be called as early as possible, even before main, because static initializers may use unavailable instructions.
void checkRequiredInstructions()
{
    struct sigaction sa{};
    struct sigaction sa_old{};
    sa.sa_sigaction = sigIllCheckHandler;
    sa.sa_flags = SA_SIGINFO;
    auto signal = SIGILL;
    if (sigemptyset(&sa.sa_mask) != 0
        || sigaddset(&sa.sa_mask, signal) != 0
        || sigaction(signal, &sa, &sa_old) != 0)
    {
        /// You may wonder about strlen.
        /// Typical implementation of strlen is using SSE4.2 or AVX2.
        /// But this is not the case because it's compiler builtin and is executed at compile time.

        writeError("Can not set signal handler\n");
        _Exit(1);
    }

    volatile InstructionFail fail = InstructionFail::NONE;

    if (sigsetjmp(jmpbuf, 1))
    {
        writeError("Instruction check fail. The CPU does not support ");
        if (!std::apply(writeRetry, instructionFailToString(fail)))
            _Exit(1);
        writeError(" instruction set.\n");
        _Exit(1);
    }

    checkRequiredInstructionsImpl(fail);

    if (sigaction(signal, &sa_old, nullptr))
    {
        writeError("Can not set signal handler\n");
        _Exit(1);
    }
}

struct Checker
{
    Checker()
    {
        checkRequiredInstructions();
    }
} checker
#ifndef OS_DARWIN
    __attribute__((init_priority(101)))    /// Run before other static initializers.
#endif
;


#if !defined(USE_MUSL)
/// NOTE: We will migrate to full static linking or our own dynamic loader to make this code obsolete.
void checkHarmfulEnvironmentVariables(char ** argv)
{
    std::initializer_list<const char *> harmful_env_variables = {
        /// The list is a selection from "man ld-linux".
        "LD_PRELOAD",
        "LD_LIBRARY_PATH",
        "LD_ORIGIN_PATH",
        "LD_AUDIT",
        "LD_DYNAMIC_WEAK",
        /// The list is a selection from "man dyld" (osx).
        "DYLD_LIBRARY_PATH",
        "DYLD_FALLBACK_LIBRARY_PATH",
        "DYLD_VERSIONED_LIBRARY_PATH",
        "DYLD_INSERT_LIBRARIES",
    };

    bool require_reexec = false;
    for (const auto * var : harmful_env_variables)
    {
        if (const char * value = getenv(var); value && value[0]) // NOLINT(concurrency-mt-unsafe)
        {
            /// NOTE: setenv() is used over unsetenv() since unsetenv() marked as harmful
            if (setenv(var, "", true)) // NOLINT(concurrency-mt-unsafe) // this is safe if not called concurrently
            {
                fmt::print(stderr, "Cannot override {} environment variable", var);
                _exit(1);
            }
            require_reexec = true;
        }
    }

    if (require_reexec)
    {
        /// Use execvp() over execv() to search in PATH.
        ///
        /// This should be safe, since:
        /// - if argv[0] is relative path - it is OK
        /// - if argv[0] has only basename, the it will search in PATH, like shell will do.
        ///
        /// Also note, that this (search in PATH) because there is no easy and
        /// portable way to get absolute path of argv[0].
        /// - on linux there is /proc/self/exec and AT_EXECFN
        /// - but on other OSes there is no such thing (especially on OSX).
        ///
        /// And since static linking will be done someday anyway,
        /// let's not pollute the code base with special cases.
        int error = execvp(argv[0], argv);
        _exit(error);
    }
}
#endif


#if defined(SANITIZE_COVERAGE)
__attribute__((no_sanitize("coverage")))
void dumpCoverage()
{
    /// A user can request to dump the coverage information into files at exit.
    /// This is useful for non-server applications such as clickhouse-format or clickhouse-client,
    /// that cannot introspect it with SQL functions at runtime.

    /// The CLICKHOUSE_WRITE_COVERAGE environment variable defines a prefix for a filename 'prefix.pid'
    /// containing the list of addresses of covered .

    /// The format is even simpler than Clang's "sancov": an array of 64-bit addresses, native byte order, no header.

    if (const char * coverage_filename_prefix = getenv("CLICKHOUSE_WRITE_COVERAGE")) // NOLINT(concurrency-mt-unsafe)
    {
        auto dump = [](const std::string & name, auto span)
        {
            /// Write only non-zeros.
            std::vector<uintptr_t> data;
            data.reserve(span.size());
            for (auto addr : span)
                if (addr)
                    data.push_back(addr);

            int fd = ::open(name.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0400);
            if (-1 == fd)
            {
                writeError("Cannot open a file to write the coverage data\n");
            }
            else
            {
                if (!writeRetry(fd, reinterpret_cast<const char *>(data.data()), data.size() * sizeof(data[0])))
                    writeError("Cannot write the coverage data to a file\n");
                if (0 != ::close(fd))
                    writeError("Cannot close the file with coverage data\n");
            }
        };

        dump(fmt::format("{}.{}", coverage_filename_prefix, getpid()), getCumulativeCoverage());
    }
}
#endif

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
