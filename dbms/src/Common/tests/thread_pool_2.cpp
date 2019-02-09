#include <atomic>
#include <iostream>
#include <Common/ThreadPool.h>


int main(int, char **)
{
    std::atomic<size_t> res{0};

    for (size_t i = 0; i < 1000; ++i)
    {
        size_t threads = 16;
        ThreadPool pool(threads);
        for (size_t j = 0; j < threads; ++j)
            pool.schedule([&]{ ++res; });
        pool.wait();
    }

    std::cerr << res << "\n";
    return 0;
}
