#include <signal.h>
#include <setjmp.h>
#include <unistd.h>

#ifdef __linux__
#include <sys/mman.h>
#endif

#include <sys/types.h>
#include <pwd.h>
#include <grp.h>

#include <new>
#include <iostream>
#include <vector>
#include <string>
#include <tuple>
#include <utility> /// pair

#include <fmt/format.h>

#include "config_tools.h"

#include <Common/StringUtils/StringUtils.h>
#include <Common/getHashOfLoadedBinary.h>
#include <Common/IO.h>
#include <IO/ReadHelpers.h>

#include <base/phdr_cache.h>
#include <base/scope_guard.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int SYSTEM_ERROR;
        extern const int BAD_ARGUMENTS;
    }
}

/// Universal executable for various clickhouse applications
#if ENABLE_CLICKHOUSE_SERVER
int mainEntryClickHouseServer(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_CLIENT
int mainEntryClickHouseClient(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_LOCAL
int mainEntryClickHouseLocal(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_BENCHMARK
int mainEntryClickHouseBenchmark(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG
int mainEntryClickHouseExtractFromConfig(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_COMPRESSOR
int mainEntryClickHouseCompressor(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_FORMAT
int mainEntryClickHouseFormat(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_COPIER
int mainEntryClickHouseClusterCopier(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_OBFUSCATOR
int mainEntryClickHouseObfuscator(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_GIT_IMPORT
int mainEntryClickHouseGitImport(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_KEEPER
int mainEntryClickHouseKeeper(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_KEEPER_CONVERTER
int mainEntryClickHouseKeeperConverter(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_STATIC_FILES_DISK_UPLOADER
int mainEntryClickHouseStaticFilesDiskUploader(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_INSTALL
int mainEntryClickHouseInstall(int argc, char ** argv);
int mainEntryClickHouseStart(int argc, char ** argv);
int mainEntryClickHouseStop(int argc, char ** argv);
int mainEntryClickHouseStatus(int argc, char ** argv);
int mainEntryClickHouseRestart(int argc, char ** argv);
#endif

int mainEntryClickHouseHashBinary(int, char **)
{
    /// Intentionally without newline. So you can run:
    /// objcopy --add-section .note.ClickHouse.hash=<(./clickhouse hash-binary) clickhouse
    std::cout << getHashOfLoadedBinaryHex();
    return 0;
}

namespace
{

using MainFunc = int (*)(int, char**);

#if !defined(FUZZING_MODE)

/// Add an item here to register new application
std::pair<const char *, MainFunc> clickhouse_applications[] =
{
#if ENABLE_CLICKHOUSE_LOCAL
    {"local", mainEntryClickHouseLocal},
#endif
#if ENABLE_CLICKHOUSE_CLIENT
    {"client", mainEntryClickHouseClient},
#endif
#if ENABLE_CLICKHOUSE_BENCHMARK
    {"benchmark", mainEntryClickHouseBenchmark},
#endif
#if ENABLE_CLICKHOUSE_SERVER
    {"server", mainEntryClickHouseServer},
#endif
#if ENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG
    {"extract-from-config", mainEntryClickHouseExtractFromConfig},
#endif
#if ENABLE_CLICKHOUSE_COMPRESSOR
    {"compressor", mainEntryClickHouseCompressor},
#endif
#if ENABLE_CLICKHOUSE_FORMAT
    {"format", mainEntryClickHouseFormat},
#endif
#if ENABLE_CLICKHOUSE_COPIER
    {"copier", mainEntryClickHouseClusterCopier},
#endif
#if ENABLE_CLICKHOUSE_OBFUSCATOR
    {"obfuscator", mainEntryClickHouseObfuscator},
#endif
#if ENABLE_CLICKHOUSE_GIT_IMPORT
    {"git-import", mainEntryClickHouseGitImport},
#endif
#if ENABLE_CLICKHOUSE_KEEPER
    {"keeper", mainEntryClickHouseKeeper},
#endif
#if ENABLE_CLICKHOUSE_KEEPER_CONVERTER
    {"keeper-converter", mainEntryClickHouseKeeperConverter},
#endif
#if ENABLE_CLICKHOUSE_INSTALL
    {"install", mainEntryClickHouseInstall},
    {"start", mainEntryClickHouseStart},
    {"stop", mainEntryClickHouseStop},
    {"status", mainEntryClickHouseStatus},
    {"restart", mainEntryClickHouseRestart},
#endif
#if ENABLE_CLICKHOUSE_STATIC_FILES_DISK_UPLOADER
    {"static-files-disk-uploader", mainEntryClickHouseStaticFilesDiskUploader},
#endif
    {"hash-binary", mainEntryClickHouseHashBinary},
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
#endif


enum class InstructionFail
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
    }
    __builtin_unreachable();
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
#ifndef __APPLE__
    __attribute__((init_priority(101)))    /// Run before other static initializers.
#endif
;


/// ClickHouse can drop privileges at startup. It is controlled by environment variables.
void setUserAndGroup()
{
    using namespace DB;

    static constexpr size_t buf_size = 16384; /// Linux man page says it is enough. Nevertheless, we will check if it's not enough and throw.
    std::unique_ptr<char[]> buf(new char[buf_size]);

    const char * env_uid = getenv("CLICKHOUSE_SETUID");
    if (env_uid && env_uid[0])
    {
        /// Is it numeric id or name?
        uid_t uid = 0;
        if (!tryParse(uid, env_uid) || uid == 0)
        {
            passwd entry{};
            passwd * result{};

            if (0 != getpwnam_r(env_uid, &entry, buf.get(), buf_size, &result))
                throwFromErrno(fmt::format("Cannot do 'getpwnam_r' to obtain uid from user name, specified in the CLICKHOUSE_SETUID environment variable ({})", env_uid), ErrorCodes::SYSTEM_ERROR);

            if (!result)
                throw Exception("User {} specified in the CLICKHOUSE_SETUID environment variable is not found in the system", ErrorCodes::BAD_ARGUMENTS);

            uid = entry.pw_uid;
        }

        if (uid == 0)
            throw Exception("User specified in the CLICKHOUSE_SETUID environment variable has id 0, but dropping privileges to uid 0 does not make sense", ErrorCodes::BAD_ARGUMENTS);

        if (0 != setuid(uid))
            throwFromErrno(fmt::format("Cannot do 'setuid' to user, specified in the CLICKHOUSE_SETUID environment variable ({})", env_uid), ErrorCodes::SYSTEM_ERROR);
    }

    const char * env_gid = getenv("CLICKHOUSE_SETGID");
    if (env_gid && env_gid[0])
    {
        gid_t gid = 0;
        if (!tryParse(gid, env_gid) || gid == 0)
        {
            group entry{};
            group * result{};

            if (0 != getgrnam_r(env_gid, &entry, buf.get(), buf_size, &result))
                throwFromErrno(fmt::format("Cannot do 'getgrnam_r' to obtain gid from group name, specified in the CLICKHOUSE_SETGID environment variable ({})", env_gid), ErrorCodes::SYSTEM_ERROR);

            if (!result)
                throw Exception("Group {} specified in the CLICKHOUSE_SETGID environment variable is not found in the system", ErrorCodes::BAD_ARGUMENTS);

            gid = entry.gr_gid;
        }

        if (gid == 0)
            throw Exception("Group specified in the CLICKHOUSE_SETGID environment variable has id 0, but dropping privileges to gid 0 does not make sense", ErrorCodes::BAD_ARGUMENTS);

        if (0 != setgid(gid))
            throwFromErrno(fmt::format("Cannot do 'setgid' to user, specified in the CLICKHOUSE_SETGID environment variable ({})", env_gid), ErrorCodes::SYSTEM_ERROR);
    }
}


/// NOTE: We will migrate to full static linking or our own dynamic loader to make this code obsolete.
void checkHarmfulEnvironmentVariables()
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

    for (const auto * var : harmful_env_variables)
    {
        if (const char * value = getenv(var); value && value[0])
        {
            std::cerr << fmt::format("Environment variable {} is set to {}. It can compromise security.\n", var, value);
            _exit(1);
        }
    }
}

}


/// This allows to implement assert to forbid initialization of a class in static constructors.
/// Usage:
///
/// extern bool inside_main;
/// class C { C() { assert(inside_main); } };
#ifndef FUZZING_MODE
bool inside_main = false;
#else
bool inside_main = true;
#endif

#if !defined(FUZZING_MODE)
int main(int argc_, char ** argv_)
{
    inside_main = true;
    SCOPE_EXIT({ inside_main = false; });

    /// PHDR cache is required for query profiler to work reliably
    /// It also speed up exception handling, but exceptions from dynamically loaded libraries (dlopen)
    ///  will work only after additional call of this function.
    updatePHDRCache();

    /// Drop privileges if needed.
    try
    {
        setUserAndGroup();
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage("setUserAndGroup", false) << '\n';
        return 1;
    }

    checkHarmfulEnvironmentVariables();

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

    return main_func(static_cast<int>(argv.size()), argv.data());
}
#endif
