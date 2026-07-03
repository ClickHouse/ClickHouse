#include <thread>
#include <iostream>
#include <atomic>

#include <base/sleep.h>

#include <IO/ReadHelpers.h>

#include <Common/ThreadFuzzer.h>


/** Proves that ThreadFuzzer helps to find concurrency bugs.
  *
  * for i in {1..10}; do ./chaos_sanitizer 1000000; done
  * for i in {1..10}; do THREAD_FUZZER_CPU_TIME_PERIOD_US=1000 THREAD_FUZZER_SLEEP_PROBABILITY=0.1 THREAD_FUZZER_SLEEP_TIME_US_MAX=100000 ./chaos_sanitizer 1000000; done
  */
int main(int argc, char ** argv)
{
    const size_t num_iterations = argc >= 2 ? DB::parse<size_t>(argv[1]) : 1000000000;

    std::cerr << (DB::ThreadFuzzer::instance().isEffective() ? "ThreadFuzzer is enabled.\n" : "ThreadFuzzer is not enabled.\n");

    std::atomic<size_t> counter1 = 0;
    std::atomic<size_t> counter2 = 0;

    /// These threads are synchronized by sleep (that's intentionally incorrect).

    std::thread t1([&]
    {
        for (size_t i = 0; i < num_iterations; ++i)
            counter1.store(counter1.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);

        sleepForNanoseconds(100000000);

        for (size_t i = 0; i < num_iterations; ++i)
            counter2.store(counter2.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    });

    std::thread t2([&]
    {
        for (size_t i = 0; i < num_iterations; ++i)
            counter2.store(counter2.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);

        sleepForNanoseconds(100000000);

        for (size_t i = 0; i < num_iterations; ++i)
            counter1.store(counter1.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    });

    t1.join();
    t2.join();

    std::cerr << "Result: " << counter1 << ", " << counter2 << "\n";

    return 0;
}
