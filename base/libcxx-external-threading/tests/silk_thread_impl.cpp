#include <silk/fibers/event.h>
#include <silk/fibers/fiber.h>
#include <silk/util/init.h>

#include <atomic>
#include <bit>
#include <cstdint>
#include <cstdlib>
#include <print>
#include <string_view>
#include <utility>

#include <pthread.h>
#include <sched.h>

/// Libc++ has unit tests that we use to verify that our external threading implementation
/// follows the contract correctly.
/// We want to run those tests in three modes:
/// 1. In plain threads only.
/// 2. In Silk fibers only.
/// 3. Plain threads and Silk fibers mixed. Main thread is fiber, then we alternate.
///
/// For (2) and (3), we don't need to copy-paste any tests code.
/// We just override __libcpp_thread_create (and a few more methods) to start fibers instead of threads.
/// We also need to run the main function in a fiber, and for that we declare __real_main and define __wrap_main
/// and use --wrap=main.

extern "C" int __real_main(int argc, char ** argv);
extern "C" int __wrap_main(int argc, char ** argv);

namespace
{

struct ThreadContext;

struct FiberParameters
{
    ThreadContext * thread;
};

constinit bool scheduler_started = false;
constinit bool mixed = false;
constinit std::atomic<uint64_t> created_threads = 0;
constinit std::atomic<void (*)(void *)> key_destructors[PTHREAD_KEYS_MAX] = {};

bool isNextThreadFiber()
{
    if (!scheduler_started)
        return false;
    return !mixed || created_threads++ % 2 == 0;
}

/// The state behind one std::thread handle, backed by either an OS thread or a fiber.
struct ThreadContext
{
    /// What the thread runs.
    void * (*function)(void *);
    void * argument;

    /// Zero for a fiber.
    pthread_t os_thread = 0;

    /// Published by the thread itself, because a fiber learns its id only once it runs.
    std::__libcpp_thread_id id = 0;
    silk::FiberEvent started{};
    silk::FiberEvent finished{};

    /// Values of the thread-specific storage keys, used only by fibers.
    void * fiber_specific[PTHREAD_KEYS_MAX] = {};

    /// The context of the fiber the caller runs in, or nullptr outside a fiber.
    static ThreadContext * currentFiber()
    {
        if (!silk::FiberScheduler::getCurrentFiberId().raw)
            return nullptr;
        return static_cast<FiberParameters *>(silk::FiberScheduler::getFiberParameters(silk::FiberScheduler::getCurrentFiber()))->thread;
    }

    void run()
    {
        id = std::__libcpp_thread_get_current_id();
        started.set();
        function(argument);
        destroyFiberSpecific();
        finished.set();
    }

    void destroyFiberSpecific()
    {
        for (size_t key = 0; key < PTHREAD_KEYS_MAX; ++key)
            if (void * value = std::exchange(fiber_specific[key], nullptr))
                if (void (*destructor)(void *) = key_destructors[key])
                    destructor(value);
    }
};

void * runInOsThread(void * thread)
{
    static_cast<ThreadContext *>(thread)->run();
    return nullptr;
}

int runInFiber(FiberParameters * parameters) noexcept
{
    parameters->thread->run();
    return 0;
}

struct MainCall
{
    int argc;
    char ** argv;
    int result;
};

void * runMain(void * argument)
{
    auto * call = static_cast<MainCall *>(argument);
    call->result = __real_main(call->argc, call->argv);
    return nullptr;
}

}

_LIBCPP_BEGIN_NAMESPACE_STD

int __libcpp_thread_create(__libcpp_thread_t * t, void * (*func)(void *), void * arg)
{
    auto * thread = new (std::malloc(sizeof(ThreadContext))) ThreadContext{.function = func, .argument = arg};
    const int r = isNextThreadFiber()
        ? silk::FiberScheduler::run(runInFiber, FiberParameters{thread}, 0, nullptr)
        : pthread_create(&thread->os_thread, nullptr, runInOsThread, thread);
    if (!r)
        *t = std::bit_cast<__libcpp_thread_t>(thread);
    return r;
}

__libcpp_thread_id __libcpp_thread_get_id(const __libcpp_thread_t * t)
{
    if (!*t)
        return 0;
    auto * thread = std::bit_cast<ThreadContext *>(*t);
    thread->started.wait();
    return thread->id;
}

int __libcpp_thread_join(__libcpp_thread_t * t)
{
    auto * thread = std::bit_cast<ThreadContext *>(*t);
    thread->finished.wait();
    return thread->os_thread ? pthread_join(thread->os_thread, nullptr) : 0;
}

int __libcpp_thread_detach(__libcpp_thread_t * t)
{
    auto * thread = std::bit_cast<ThreadContext *>(*t);
    return thread->os_thread ? pthread_detach(thread->os_thread) : 0;
}

int __libcpp_tls_create(__libcpp_tls_key * key, void (*at_exit)(void *))
{
    const int r = pthread_key_create(key, at_exit);
    if (!r)
        key_destructors[*key] = at_exit;
    return r;
}

void * __libcpp_tls_get(__libcpp_tls_key key)
{
    if (ThreadContext * fiber = ThreadContext::currentFiber())
        return fiber->fiber_specific[key];
    return pthread_getspecific(key);
}

int __libcpp_tls_set(__libcpp_tls_key key, void * p)
{
    if (ThreadContext * fiber = ThreadContext::currentFiber())
    {
        fiber->fiber_specific[key] = p;
        return 0;
    }
    return pthread_setspecific(key, p);
}

_LIBCPP_END_NAMESPACE_STD

extern "C" int __wrap_main(int argc, char ** argv)
{
    const char * mode = std::getenv("LIBCXX_TEST_THREADS");
    if (!mode || (mode != std::string_view("fiber") && mode != std::string_view("mixed")))
    {
        std::println(stderr, "LIBCXX_TEST_THREADS must be fiber or mixed");
        return 1;
    }
    mixed = mode == std::string_view("mixed");

    silk::initialize();
    silk::FiberScheduler::Options options;
    options.fiberStackSize = 1024 * 1024;
    CPU_ZERO(&options.cpuMask);
    CPU_SET(0, &options.cpuMask);
    CPU_SET(1, &options.cpuMask);
    silk::FiberScheduler::initialize(&options);
    scheduler_started = true;

    MainCall main_call{.argc = argc, .argv = argv, .result = 0};
    std::__libcpp_thread_t main_thread;
    if (std::__libcpp_thread_create(&main_thread, runMain, &main_call))
        std::abort();
    std::__libcpp_thread_join(&main_thread);
    std::_Exit(main_call.result);
}
