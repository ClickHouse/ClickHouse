#pragma once

#include <Common/ThreadPool_fwd.h>
#include <cstdlib>
#include <memory>

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


/*
 * ThreadPool used for the Backup IO.
 */
class BackupsIOThreadPool
{
    static std::unique_ptr<ThreadPool> instance;

public:
    static void initialize(size_t max_threads, size_t max_free_threads, size_t queue_size);
    static ThreadPool & get();
};


/*
 * ThreadPool used for the loading of Outdated data parts for MergeTree tables.
 */
class OutdatedPartsLoadingThreadPool
{
    static std::unique_ptr<ThreadPool> instance;

public:
    static void initialize(size_t max_threads, size_t max_free_threads, size_t queue_size);
    static ThreadPool & get();
};

}
