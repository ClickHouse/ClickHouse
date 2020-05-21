#pragma once

#include <memory>
#include <thread>

void parallelMemcpy(char * __restrict x, const char * __restrict y, const size_t n, const size_t threads_num)
{
    std::thread t[threads_num - 1];

    for (size_t id = 0; id < threads_num - 1; ++id)
    {
        size_t start = (id * n) / threads_num;
        size_t size = ((id + 1) * n) / threads_num - start;

        t[id] = std::thread([=](){ memcpy(x + start, y + start, size); });
    }

    {
        size_t start = ((threads_num - 1) * n) / threads_num;
        size_t size = (threads_num * n) / threads_num - start;

        memcpy(x + start, y + start, size);
    }

    for (size_t id = 0; id < threads_num - 1; ++id)
        t[id].join();
}
