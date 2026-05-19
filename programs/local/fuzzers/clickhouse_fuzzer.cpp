#include <base/phdr_cache.h>
#include <base/scope_guard.h>
#include <base/defines.h>

#include <Client/ClientBase.h>
#include <Common/EnvironmentChecks.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/getHashOfLoadedBinary.h>
#include <Common/Crypto/OpenSSLInitializer.h>

#include <boost/algorithm/string/split.hpp>


#include <csignal>
#include <pthread.h>
#include <sanitizer/common_interface_defs.h>
#include <sys/poll.h>
#include <unistd.h>

#include <new>
#include <string_view>

#ifdef SANITIZER
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"
extern "C" {
#ifdef ADDRESS_SANITIZER
const char * __asan_default_options()
{
    return "halt_on_error=1 abort_on_error=1";
}
const char * __lsan_default_options()
{
    return "max_allocation_size_mb=32768";
}
#endif

#ifdef MEMORY_SANITIZER
const char * __msan_default_options()
{
    return "abort_on_error=1 poison_in_dtor=1 max_allocation_size_mb=32768";
}
#endif

#ifdef THREAD_SANITIZER
const char * __tsan_default_options()
{
    return "halt_on_error=1 abort_on_error=1 history_size=7 second_deadlock_stack=1 max_allocation_size_mb=32768";
}
#endif

#ifdef UNDEFINED_BEHAVIOR_SANITIZER
const char * __ubsan_default_options()
{
    return "print_stacktrace=1 max_allocation_size_mb=32768";
}
#endif
}
#pragma clang diagnostic pop
#endif

int mainEntryClickHouseLocal(int argc, char ** argv);

namespace
{

using MainFunc = int (*)(int, char**);

}

/// Prevent messages from JeMalloc in the release build.
/// Some of these messages are non-actionable for the users, such as:
/// <jemalloc>: Number of CPUs detected is not deterministic. Per-CPU arena disabled.
#if USE_JEMALLOC && defined(NDEBUG) && !defined(SANITIZER)
extern "C" void (*je_malloc_message)(void *, const char *s);
__attribute__((constructor(0))) void init_je_malloc_message() { je_malloc_message = [](void *, const char *){}; }
#elif USE_JEMALLOC
#include <unordered_set>
/// Ignore messages which can be safely ignored, e.g. EAGAIN on pthread_create
extern "C" void (*je_malloc_message)(void *, const char * s);
__attribute__((constructor(0))) void init_je_malloc_message()
{
    je_malloc_message = [](void *, const char * str)
    {
        using namespace std::literals;
        static const std::unordered_set<std::string_view> ignore_messages{
            "<jemalloc>: background thread creation failed (11)\n"sv};

        std::string_view message_view{str};
        if (ignore_messages.contains(message_view))
            return;

#    if defined(SYS_write)
        syscall(SYS_write, 2 /*stderr*/, message_view.data(), message_view.size());
#    else
        write(STDERR_FILENO, message_view.data(), message_view.size());
#    endif
    };
}
#endif

/// OpenSSL early initialization.
/// See also EnvironmentChecks.cpp for other static initializers.
/// Must be ran after EnvironmentChecks.cpp, as OpenSSL uses SSE4.1 and POPCNT.
__attribute__((constructor(202))) void init_ssl()
{
    DB::OpenSSLInitializer::instance();
}

/// This allows to implement assert to forbid initialization of a class in static constructors.
/// Usage:
///
/// extern bool inside_main;
/// class C { C() { assert(inside_main); } };
bool inside_main = false;

int clickhouseMain(int argc_, char ** argv_)
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

    int exit_code = mainEntryClickHouseLocal(argc_, argv_);

    return exit_code;
}

bool isMerge(int argc, const char * const * argv)
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

std::mutex mutex;
std::condition_variable cv;
enum class FuzzerState
{
    NONE,
    WAITING_FOR_INPUT,
    DATA_READY,
    FINISHED
};
std::atomic<FuzzerState> state{FuzzerState::NONE};
String query;

std::optional<std::thread> runner;
pthread_t runner_thread_id{};
struct sigaction original_sigalrm_action{};

String clickhouse{"clickhouse"};
std::vector<char *> clickhouse_args{clickhouse.data()};

/// Signal-safe stderr print helper.
void signalSafeWrite(const char * msg)
{
    (void)write(STDERR_FILENO, msg, __builtin_strlen(msg));
}

/// Flag set by the SIGUSR1 handler on the runner thread after printing its stack.
std::atomic<bool> runner_stack_printed{false};

/// SIGUSR1 handler installed on the runner thread — prints its own stack trace.
void runnerStackTraceHandler(int /*sig*/, siginfo_t * /*info*/, void * /*context*/)
{
    signalSafeWrite("[fuzzer] SIGUSR1 handler entered on runner thread\n");
    signalSafeWrite("\n=== Runner thread stack trace (where the query is stuck) ===\n");
    __sanitizer_print_stack_trace();
    signalSafeWrite("=== End runner thread stack trace ===\n\n");
    runner_stack_printed.store(true, std::memory_order_release);
}

inline void signal_safe_sleep_ms(int ms)
{
     (void)poll(nullptr, 0, ms);
}

/// When libfuzzer's timeout fires SIGALRM, it lands on the main thread.
/// We first dump the runner thread's stack trace (via SIGUSR1), then call
/// libfuzzer's original handler which will print the main thread stack and _Exit.
void fuzzerSigalrmHandler(int /*sig*/, siginfo_t * /*info*/, void * /*context*/)
{
    signalSafeWrite("[fuzzer] SIGALRM handler: sending SIGUSR1 to runner thread\n");

    /// Send SIGUSR1 to the runner thread to print its stack trace.
    runner_stack_printed.store(false, std::memory_order_release);
    int rc = pthread_kill(runner_thread_id, SIGUSR1);

    if (rc != 0)
    {
        signalSafeWrite("[fuzzer] pthread_kill failed with error ");
        /// Signal-safe integer-to-string for small error codes.
        char err_buf[16];
        int pos = 14;
        err_buf[15] = '\0';
        err_buf[14] = '\n';
        int tmp = rc;
        do
        {
            err_buf[--pos] = '0' + (tmp % 10);
            tmp /= 10;
        } while (tmp > 0 && pos > 0);
        signalSafeWrite(err_buf + pos);
    }

    /// Wait for the runner thread to print its stack, with nanosleep to yield CPU.
    constexpr int max_iterations = 3000;
    int i = 0;
    for (; i < max_iterations && !runner_stack_printed.load(std::memory_order_acquire); ++i)
        signal_safe_sleep_ms(1);

    if (i == max_iterations)
        signalSafeWrite("[fuzzer] Timed out waiting for runner thread stack trace\n");
    else
        signalSafeWrite("[fuzzer] Runner thread stack trace printed successfully\n");

    /// Restore and call libfuzzer's original SIGALRM handler.
    /// It will print the main thread's stack and call _Exit.
    (void)sigaction(SIGALRM, &original_sigalrm_action, nullptr);
    (void)raise(SIGALRM);
}

extern "C"
int LLVMFuzzerInitialize(const int *argc, char ***argv)
{
    // If it's a merge coordinator don't start anything
    if (isMerge(*argc, *argv))
        return 0;

    // Initialize as a main thread
    DB::MainThreadStatus::getInstance();

    // Collect clickhouse arguments
    bool ignore = false;
    for (int i = 1; i < *argc; ++i)
        if (ignore)
            clickhouse_args.push_back((*argv)[i]);
        else
            if (std::string_view arg{(*argv)[i]}; arg.substr(0, arg.find('=')) == "-ignore_remaining_args")
                ignore = true;

    {
        // Start clickhouse local
        std::unique_lock lock(mutex);
        runner = std::thread(clickhouseMain, clickhouse_args.size(), clickhouse_args.data());
        runner_thread_id = runner->native_handle();
        if (!cv.wait_for(lock, std::chrono::seconds(30), []{ return state == FuzzerState::WAITING_FOR_INPUT; }))
            abort();
    }

    int ret = std::atexit([]()
    {
        {
            std::lock_guard lock(mutex);
            state = FuzzerState::FINISHED;
        }
        cv.notify_one();
        if (runner.has_value())
            runner->join();
    });

    if (ret != 0)
        abort();

    return 0;
}

extern "C"
int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    /// Install our SIGALRM forwarder on the first call, after libfuzzer
    /// has already set up its own handler (which we save as original).
    static bool handler_installed = false;
    if (!handler_installed)
    {
        struct sigaction sa = {};
        sa.sa_sigaction = fuzzerSigalrmHandler;
        sa.sa_flags = SA_SIGINFO;
        sigaction(SIGALRM, &sa, &original_sigalrm_action);
        handler_installed = true;
    }

    {
        std::unique_lock lock(mutex);
        cv.wait(lock, []{ return state == FuzzerState::WAITING_FOR_INPUT; });
        query = {reinterpret_cast<const char *>(data), size};
        state = FuzzerState::DATA_READY;
        cv.notify_one();

        /// Wait for the runner to finish processing, so that libfuzzer's
        /// SIGALRM fires while we are inside the callback (RunningUserCallback=true).
        /// This way the timeout mechanism works correctly, and our handler
        /// can dump the runner thread's stack before libfuzzer exits.
        cv.wait(lock, []{ return state == FuzzerState::WAITING_FOR_INPUT || state == FuzzerState::FINISHED; });
    }

    return 0;
}

void DB::ClientBase::runLibFuzzer()
{
    // Initialize thread_status
    ThreadStatus thread_status;

    /// Install SIGUSR1 handler on the runner thread for stack trace dumping.
    {
        struct sigaction sa = {};
        sa.sa_sigaction = runnerStackTraceHandler;
        sa.sa_flags = SA_SIGINFO;
        (void)sigaction(SIGUSR1, &sa, nullptr);
    }

    {
        std::lock_guard lock(mutex);
        state = FuzzerState::WAITING_FOR_INPUT;
    }
    cv.notify_one();

    while (true)
    {
        {
            std::unique_lock lock(mutex);
            cv.wait(lock, []{ return state == FuzzerState::FINISHED || state == FuzzerState::DATA_READY; });
            if (state == FuzzerState::FINISHED)
                break;

            try
            {
                processQueryText(query);
            }
            catch (const Exception &)
            {
                /// Normal query errors (syntax, runtime, etc.) — ignore, same as
                /// interactive mode in runInteractive. The bulk of query exceptions
                /// are already caught inside executeMultiQuery and never reach here.
            }
            catch (...)
            {
                /// Non-DB exception indicates a real bug. Log it and abort so that
                /// libfuzzer registers a crash with a meaningful message.
                /// (We can't rely on std::terminate because the stack is already
                /// unwound by then, losing the exception info.)
                signalSafeWrite("[fuzzer] Non-DB::Exception caught in runner thread: ");
                signalSafeWrite(getCurrentExceptionMessage(true).c_str());
                signalSafeWrite("\n");
                abort();
            }
            state = FuzzerState::WAITING_FOR_INPUT;
        }
        cv.notify_one();
    }
}
