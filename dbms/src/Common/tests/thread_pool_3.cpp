#include <mutex>
#include <iostream>
#include <Common/ThreadPool.h>

/// Test for thread self-removal when number of free threads in pool is too large.
/// Just checks that nothing weird happens.

template <typename Pool>
void test()
{
    Pool pool(10, 2, 10);

    std::mutex mutex;
    for (size_t i = 0; i < 10; ++i)
        pool.schedule([&]{ std::lock_guard lock(mutex); std::cerr << '.'; });
    pool.wait();
}

int main(int, char **)
{
    test<FreeThreadPool>();
    std::cerr << '\n';
    test<ThreadPool>();
    std::cerr << '\n';

    return 0;
}
