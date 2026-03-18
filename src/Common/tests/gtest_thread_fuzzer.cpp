#include <optional>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include <Common/ThreadFuzzer.h>
#include <Common/Stopwatch.h>

TEST(ThreadFuzzer, mutex)
{
    /// Initialize ThreadFuzzer::started
    DB::ThreadFuzzer::instance().isEffective();

    std::mutex mutex;
    std::atomic<size_t> elapsed_ns = 0;

    auto func = [&]()
    {
        Stopwatch watch;
        for (size_t i = 0; i < 1e6; ++i)
        {
            mutex.lock();
            mutex.unlock();
        }
        elapsed_ns += watch.elapsedNanoseconds();
    };

    std::vector<std::optional<std::thread>> threads(10);

    for (auto & thread : threads)
        thread.emplace(func);

    for (auto & thread : threads)
        thread->join();

    std::cout << "elapsed: " << elapsed_ns/1e9 << "\n";
}
