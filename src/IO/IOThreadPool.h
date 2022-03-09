#pragma once

#include <Common/ThreadPool.h>

namespace DB
{

/*
 * ThreadPool used for the IO.
 */
class IOThreadPool
{
    static std::unique_ptr<ThreadPool> instance;

public:
    static void initialize(size_t max_threads, size_t max_free_threads, size_t queue_size);
    static ThreadPool & get();
};

}
