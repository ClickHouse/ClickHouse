#include <common/ThreadPool.h>

/** Reproduces bug in ThreadPool.
  * It get stuck if we call 'wait' many times from many other threads simultaneously.
  */


int main(int argc, char ** argv)
{
    auto worker = []
    {
        for (size_t i = 0; i < 100000000; ++i)
            __asm__ volatile ("nop");
    };

    constexpr size_t num_threads = 4;
    constexpr size_t num_jobs = 4;

    ThreadPool pool(num_threads);

    for (size_t i = 0; i < num_jobs; ++i)
        pool.schedule(worker);

    constexpr size_t num_waiting_threads = 4;

    ThreadPool waiting_pool(num_waiting_threads);

    for (size_t i = 0; i < num_waiting_threads; ++i)
        waiting_pool.schedule([&pool]{ pool.wait(); });

    waiting_pool.wait();

    return 0;
}
