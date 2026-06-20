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

#if defined(SANITIZE_COVERAGE)
#    include <Common/Coverage.h>
#endif

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
extern "C" void (*malloc_message)(void *, const char *s);
__attribute__((constructor(0))) void init_je_malloc_message() { malloc_message = [](void *, const char *){}; }
#elif USE_JEMALLOC
#include <unordered_set>
/// Ignore messages which can be safely ignored, e.g. EAGAIN on pthread_create
extern "C" void (*malloc_message)(void *, const char * s);
__attribute__((constructor(0))) void init_je_malloc_message()
{
    malloc_message = [](void *, const char * str)
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

#if defined(SANITIZE_COVERAGE)
    dumpCoverage();
#endif

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
String clickhouse{"clickhouse"};
std::vector<char *> clickhouse_args{clickhouse.data()};

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
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, []{ return state == FuzzerState::WAITING_FOR_INPUT; });
        query = {reinterpret_cast<const char *>(data), size};
        state = FuzzerState::DATA_READY;
    }
    cv.notify_one();

    return 0;
}

void DB::ClientBase::runLibFuzzer()
{
    // Initialize thread_status
    ThreadStatus thread_status;

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

            processQueryText(query);
            state = FuzzerState::WAITING_FOR_INPUT;
        }
        cv.notify_one();
    }
}
