#include <base/phdr_cache.h>
#include <base/scope_guard.h>
#include <base/defines.h>
#include <base/sanitizer_options.h>

#include <Client/ClientBase.h>
#include <Common/EnvironmentChecks.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/getHashOfLoadedBinary.h>
#include <Common/Crypto/OpenSSLInitializer.h>
#include <Common/ThreadStatus.h>

#include <boost/algorithm/string/split.hpp>


#include <csignal>
#include <ctime>
#include <pthread.h>
#include <sanitizer/common_interface_defs.h>
#include <sys/poll.h>
#include <unistd.h>

#include <new>
#include <string_view>

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

/// Monotonic-clock seconds at the start of the current `LLVMFuzzerTestOneInput`
/// call. libfuzzer arms `setitimer(ITIMER_REAL)` with an interval of
/// `UnitTimeoutSec / 2 + 1` seconds (11 s for our 20 s timeout), so SIGALRM
/// fires periodically — not only at the actual timeout. The wrapper uses this
/// to skip the expensive runner-stack dump when no timeout is imminent.
std::atomic<int64_t> iteration_start_sec{0};

/// libfuzzer's `-timeout=N` value, parsed in `LLVMFuzzerInitialize`. Defaults
/// to libfuzzer's own default of 1200 s. The wrapper dumps the runner stack
/// only once `elapsed_sec >= unit_timeout_sec`, i.e. when the next forwarded
/// SIGALRM is libfuzzer's actual timeout firing rather than a periodic tick.
std::atomic<int64_t> unit_timeout_sec{1200};

/// Per-iteration one-shot latch so that multiple sequential SIGALRM handler
/// invocations within the same iteration don't repeat the slow runner-stack
/// dump. Reset at the start of every `LLVMFuzzerTestOneInput` call so that
/// the wrapper still fires on a real timeout in a later iteration even if an
/// earlier periodic alarm consumed it.
std::atomic<bool> dump_started{false};

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
/// We first dump the runner thread's stack trace (via SIGUSR1), then forward
/// to libfuzzer's original handler which will print the main thread stack and
/// `_Exit`.
///
/// SIGALRM is also fired periodically by libfuzzer's `setitimer` (every
/// `UnitTimeout/2 + 1` seconds), not only on an actual timeout. We must
/// distinguish the two: the runner-stack dump shells out to llvm-symbolizer
/// (slow, not async-signal-safe), and running it on every periodic alarm has
/// caused real timeouts where the iteration would otherwise have completed in
/// time. We skip the dump unless our own monotonic clock matches libfuzzer's
/// own timeout condition (`elapsed >= UnitTimeoutSec`). We also forward to
/// libfuzzer's handler *without* permanently restoring it, so the wrapper
/// continues to intercept later periodic alarms.
void fuzzerSigalrmHandler(int sig, siginfo_t * info, void * ctx)
{
    struct timespec ts;
    (void)clock_gettime(CLOCK_MONOTONIC, &ts);
    int64_t elapsed_sec = ts.tv_sec - iteration_start_sec.load(std::memory_order_acquire);

    /// libfuzzer's `AlarmCallback` itself only declares a timeout once
    /// `elapsed >= UnitTimeoutSec`. Use the same condition so the periodic
    /// SIGALRMs at `UnitTimeoutSec / 2 + 1` seconds pass through untouched
    /// regardless of the `-timeout=N` value. The next forwarded SIGALRM
    /// after this check is libfuzzer's actual timeout that will `_Exit`.
    if (elapsed_sec >= unit_timeout_sec.load(std::memory_order_acquire))
    {
        bool expected = false;
        if (dump_started.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
        {
            signalSafeWrite("[fuzzer] SIGALRM handler: sending SIGUSR1 to runner thread\n");

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

            /// Wait for the runner thread to print its stack, yielding CPU.
            constexpr int max_iterations = 3000;
            int i = 0;
            for (; i < max_iterations && !runner_stack_printed.load(std::memory_order_acquire); ++i)
                signal_safe_sleep_ms(1);

            if (i == max_iterations)
                signalSafeWrite("[fuzzer] Timed out waiting for runner thread stack trace\n");
            else
                signalSafeWrite("[fuzzer] Runner thread stack trace printed successfully\n");
        }
    }

    /// Forward to libfuzzer's original SIGALRM handler. If this is a real
    /// timeout it will print the main-thread stack and `_Exit`; otherwise it
    /// returns and the wrapper stays installed for the next periodic alarm.
    if (original_sigalrm_action.sa_flags & SA_SIGINFO)
    {
        if (original_sigalrm_action.sa_sigaction)
            original_sigalrm_action.sa_sigaction(sig, info, ctx);
    }
    else if (original_sigalrm_action.sa_handler != SIG_IGN
          && original_sigalrm_action.sa_handler != SIG_DFL)
    {
        original_sigalrm_action.sa_handler(sig);
    }
}

extern "C"
int LLVMFuzzerInitialize(const int *argc, char ***argv)
{
    // If it's a merge coordinator don't start anything
    if (isMerge(*argc, *argv))
        return 0;

    // Initialize as a main thread
    DB::MainThreadStatus::getInstance();

    // Collect clickhouse arguments and pick up libfuzzer's `-timeout=N` so
    // the SIGALRM wrapper can match libfuzzer's own timeout condition exactly.
    bool ignore = false;
    for (int i = 1; i < *argc; ++i)
        if (ignore)
            clickhouse_args.push_back((*argv)[i]);
        else
        {
            std::string_view arg{(*argv)[i]};
            std::string_view flag{arg.begin(), std::ranges::find(arg, '=')};
            if (flag == "-ignore_remaining_args")
                ignore = true;
            else if (flag == "-timeout" && flag.size() < arg.size())
            {
                int64_t t = std::atoi(arg.data() + flag.size() + 1);
                if (t > 0)
                    unit_timeout_sec.store(t, std::memory_order_release);
            }
        }

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
    /// Mark the start of this iteration before installing/handling SIGALRM so
    /// `fuzzerSigalrmHandler` can tell a real timeout from libfuzzer's
    /// periodic SIGALRM. Also reset the one-shot dump guard so that the
    /// wrapper still fires on a real timeout in this iteration even if an
    /// earlier iteration's late `SIGALRM` already consumed it.
    {
        struct timespec ts;
        (void)clock_gettime(CLOCK_MONOTONIC, &ts);
        iteration_start_sec.store(ts.tv_sec, std::memory_order_release);
        dump_started.store(false, std::memory_order_release);
    }

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

    /// Block SIGALRM on the runner thread so libfuzzer's periodic timer
    /// signal is only delivered to the libfuzzer main thread. Without this,
    /// SIGALRM can land on the runner — it then runs `fuzzerSigalrmHandler`
    /// and `pthread_kill`'s SIGUSR1 to itself, causing the slow runner-stack
    /// dump to run concurrently with the main thread's dump and corrupting
    /// the signal-handler state via two competing `sigaction` calls.
    {
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        (void)pthread_sigmask(SIG_BLOCK, &set, nullptr);
    }

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
