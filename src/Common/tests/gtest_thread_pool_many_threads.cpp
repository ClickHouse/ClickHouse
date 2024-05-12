#include <chrono>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>

#include <gtest/gtest.h>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
extern const Metric LocalThreadScheduled;
}

using namespace std::chrono_literals;

namespace ProfileEvents
{
extern const Event GlobalThreadPoolExpansions;
extern const Event GlobalThreadPoolShrinks;
extern const Event GlobalThreadPoolJobScheduleMicroseconds;
extern const Event LocalThreadPoolExpansions;
extern const Event LocalThreadPoolShrinks;
extern const Event LocalThreadPoolJobScheduleMicroseconds;
}


void worker()
{
#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wdeprecated-volatile"
#endif

    // std::cerr << "from worker" << std::endl;

    std::this_thread::sleep_for(300us);
    // volatile UInt64 j = 0;
    // for (size_t i = 0; i < 200000; ++i)
    // {
    //     ++j;
    // }
#ifdef __clang__
#    pragma clang diagnostic pop
#endif
};


void small_query()
{
    std::this_thread::sleep_for(200ms);
    for (size_t i = 0; i < 5; ++i)
    {
        Stopwatch small_pool_watch;
        ThreadPool small_query_pool(
            CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);

        small_query_pool.scheduleOrThrowOnError(worker);
        small_query_pool.wait();
        std::cerr << "small_pool_watch.elapsedMicroseconds " << small_pool_watch.elapsedMicroseconds() << std::endl;

        std::this_thread::sleep_for(100ms);
    }
}


struct GlobalPoolType
{
    bool experimental = false;
};

class ThreadPoolTest : public ::testing::TestWithParam<GlobalPoolType>
{
};


constexpr size_t num_threads = 10000;
constexpr size_t num_jobs = 800000;

TEST_P(ThreadPoolTest, Warm)
{
    if (GetParam().experimental)
    {
        auto t_start = std::chrono::high_resolution_clock::now();
        GlobalThreadPool<tp::ThreadPool>::initialize(5000, 100, 20000);
        auto t_end = std::chrono::high_resolution_clock::now();
        double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
        std::cout << "elapsed_time_ms=" << elapsed_time_ms << std::endl;


        auto & global_pool = GlobalThreadPool<tp::ThreadPool>::instance();
        global_pool.wait();
    }
    else
    {
        auto t_start = std::chrono::high_resolution_clock::now();
        GlobalThreadPool<FreeThreadPool>::initialize(5000, 100, 20000);
        auto t_end = std::chrono::high_resolution_clock::now();
        double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
        std::cout << "elapsed_time_ms=" << elapsed_time_ms << std::endl;
        auto & global_pool = GlobalThreadPool<FreeThreadPool>::instance();
        global_pool.wait();
    }


    ThreadPool warm_pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        5000, 100, 20000);
    warm_pool.setQueueSize(0);
    warm_pool.wait();

    for (size_t i = 0; i < 1000 /* num_jobs */; ++i)
    {
        for (size_t j = 0; j < 10000 /* num_jobs */; ++j)
            warm_pool.scheduleOrThrowOnError(worker);
        worker();
    }

    warm_pool.wait();

    std::cout << std::boolalpha << DB::CurrentThread::isInitialized()
              << " GlobalThreadPoolExpansions: "
              <<  DB::CurrentThread::getProfileEvents()[ProfileEvents::GlobalThreadPoolExpansions]
              <<  std::endl
              << "GlobalThreadPoolShrinks: "
              <<  DB::CurrentThread::getProfileEvents()[ProfileEvents::GlobalThreadPoolShrinks]
              <<  std::endl
              << "GlobalThreadPoolJobScheduleMicroseconds: "
              <<  DB::CurrentThread::getProfileEvents()[ProfileEvents::GlobalThreadPoolJobScheduleMicroseconds]
              <<  std::endl
              << "LocalThreadPoolExpansions: "
              <<  DB::CurrentThread::getProfileEvents()[ProfileEvents::LocalThreadPoolExpansions]
              <<  std::endl
              << "LocalThreadPoolShrinks: "
              <<  DB::CurrentThread::getProfileEvents()[ProfileEvents::LocalThreadPoolShrinks]
              <<  std::endl
              << "LocalThreadPoolJobScheduleMicroseconds: "
              <<  DB::CurrentThread::getProfileEvents()[ProfileEvents::LocalThreadPoolJobScheduleMicroseconds]
              << std::endl;
}


TEST_P(ThreadPoolTest, ManyThreads)
{
    if (GetParam().experimental)
    {
        auto & global_pool = GlobalThreadPool<tp::ThreadPool>::instance();

        global_pool.wait();
    }
    else
    {
        auto & global_pool = GlobalThreadPool<FreeThreadPool>::instance();

        global_pool.setMaxThreads(10000);
        global_pool.setMaxFreeThreads(1000);
        global_pool.setQueueSize(10000);

        global_pool.wait();
    }


    constexpr size_t num_injectors = 10;


    {
        // warming up - create threads in GlobalPool
        ThreadPool warm_pool(
            CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, num_threads);
        warm_pool.setQueueSize(0);
        warm_pool.wait();

        for (size_t i = 0; i < num_jobs; ++i)
            warm_pool.scheduleOrThrowOnError(worker);

        warm_pool.wait();
    }


    Stopwatch pool_watch;
    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, num_threads);

    std::thread small_thread(small_query);

    size_t inject_by_injector = num_jobs / num_injectors;
    auto injector = [&pool](size_t num)
    {
        for (size_t i = 0; i < num; ++i)
            pool.scheduleOrThrowOnError(worker);
    };


    std::vector<std::thread> inject_threads;

    for (size_t i = 0; i < num_injectors; ++i)
        if (i < num_injectors - 1)
            inject_threads.emplace_back(injector, inject_by_injector);
        else
            inject_threads.emplace_back(injector, num_jobs - inject_by_injector * (num_injectors - 1));

    pool.wait();
    std::cout << "pool_watch.elapsedMicroseconds " << pool_watch.elapsedMicroseconds() << std::endl;
    for (auto & inj : inject_threads)
        inj.join();

    small_thread.join();
}


TEST_P(ThreadPoolTest, ManyThreadsNoReclaim)
{
    if (GetParam().experimental)
    {
        auto & global_pool = GlobalThreadPool<tp::ThreadPool>::instance();
        global_pool.wait();
    }
    else
    {
        auto & global_pool = GlobalThreadPool<FreeThreadPool>::instance();
        global_pool.setMaxThreads(10000);
        global_pool.setMaxFreeThreads(10000);
        global_pool.setQueueSize(10000);
        global_pool.wait();
    }

    constexpr size_t num_injectors = 10;

    {
        // warming up - create threads in GlobalPool
        ThreadPool warm_pool(
            CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, num_threads);
        warm_pool.setQueueSize(0);
        warm_pool.wait();

        for (size_t i = 0; i < num_jobs; ++i)
            warm_pool.scheduleOrThrowOnError(worker);
        warm_pool.wait();
    }


    Stopwatch pool_watch;
    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, num_threads);


    std::thread small_thread(small_query);
    size_t inject_by_injector = num_jobs / num_injectors;
    auto injector = [&pool](size_t num)
    {
        for (size_t i = 0; i < num; ++i)
            pool.scheduleOrThrowOnError(worker);
    };
    std::vector<std::thread> inject_threads;

    for (size_t i = 0; i < num_injectors; ++i)
        if (i < num_injectors - 1)
            inject_threads.emplace_back(injector, inject_by_injector);
        else
            inject_threads.emplace_back(injector, num_jobs - inject_by_injector * (num_injectors - 1));

    for (auto & inj : inject_threads)
        inj.join();
    pool.wait();
    std::cout << "pool_watch.elapsedMicroseconds " << pool_watch.elapsedMicroseconds() << std::endl;

    small_thread.join();
}

INSTANTIATE_TEST_SUITE_P(GlobalPoolManyThreads, ThreadPoolTest, testing::Values(GlobalPoolType{false}, GlobalPoolType{true}));
