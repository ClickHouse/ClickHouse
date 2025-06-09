#include <Common/EnvironmentChecks.h>
#include <Common/IO.h>

#include <fmt/format.h>

#include <csignal>
#include <csetjmp>
#include <cstdint>

#include <tuple>

#include <unistd.h>

namespace
{

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

}


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
