#include <iostream>
#include <iomanip>

#include <IO/ReadHelpers.h>

#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>


int value = 0;

static void f() { ++value; }
static void * g(void *) { f(); return {}; }

using ThreadFromGlobalPoolSimple = ThreadFromGlobalPoolImpl</* propagate_opentelemetry_context= */ false, /* global_trace_collector_allowed= */ false>;
using SimpleThreadPool = ThreadPoolImpl<ThreadFromGlobalPoolSimple>;

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace DB
{
    namespace ErrorCodes
    {
        extern const int PTHREAD_ERROR;
    }
}


template <typename F>
void test(size_t n, const char * name, F && kernel)
{
    value = 0;

    Stopwatch watch;
    Stopwatch watch_one;
    double max_seconds = 0;

    std::cerr << name << ":\n";

    for (size_t i = 0; i < n; ++i)
    {
        watch_one.restart();

        kernel();

        watch_one.stop();
        max_seconds = std::max(watch_one.elapsedSeconds(), max_seconds);
    }

    watch.stop();

    std::cerr
        << std::fixed << std::setprecision(2)
        << n << " ops in "
        << watch.elapsedSeconds() << " sec., "
        << n / watch.elapsedSeconds() << " ops/sec., "
        << "avg latency: " << watch.elapsedSeconds() / n * 1000000 << " μs, "
        << "max latency: " << max_seconds * 1000000 << " μs "
        << "(res = " << value << ")"
        << std::endl;
}


int main(int argc, char ** argv)
{
    size_t n = argc == 2 ? DB::parse<UInt64>(argv[1]) : 100000;

    test(n, "Create and destroy ThreadPool each iteration", []
    {
        SimpleThreadPool tp(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);
        tp.scheduleOrThrowOnError(f);
        tp.wait();
    });

    test(n, "pthread_create, pthread_join each iteration", []
    {
        pthread_t thread;
        if (pthread_create(&thread, nullptr, g, nullptr))
            throw DB::ErrnoException(DB::ErrorCodes::PTHREAD_ERROR, "Cannot create thread");
        if (pthread_join(thread, nullptr))
            throw DB::ErrnoException(DB::ErrorCodes::PTHREAD_ERROR, "Cannot join thread");
    });

    test(n, "Create and destroy std::thread each iteration", []
    {
        std::thread thread(f);
        thread.join();
    });

    {
        SimpleThreadPool tp(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);

        test(n, "Schedule job for Threadpool each iteration", [&tp]
        {
            tp.scheduleOrThrowOnError(f);
            tp.wait();
        });
    }

    {
        SimpleThreadPool tp(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 128);

        test(n, "Schedule job for Threadpool with 128 threads each iteration", [&tp]
        {
            tp.scheduleOrThrowOnError(f);
            tp.wait();
        });
    }

    return 0;
}
