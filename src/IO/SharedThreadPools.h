#pragma once

#include <Common/ThreadPool.h>
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
class ActivePartsLoadingThreadPool
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
    static size_t max_threads_turbo;
    static std::unique_ptr<ThreadPool> instance;

public:
    static void initialize(size_t max_threads_normal_, size_t max_threads_turbo_, size_t max_free_threads_, size_t queue_size_);
    static ThreadPool & get();
    static void turboMode();
};

}
