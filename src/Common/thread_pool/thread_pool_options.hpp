#pragma once

#include <algorithm>
#include <thread>

namespace tp
{

/**
 * @brief The ThreadPoolOptions class provides creation options for
 * ThreadPool.
 */
class ThreadPoolOptions
{
public:
    /**
     * @brief ThreadPoolOptions Construct default options for thread pool.
     */
    ThreadPoolOptions();

    /**
     * @brief setMaxThreadCount Set thread count.
     * @param count Number of threads can be created
     */
    ThreadPoolOptions & setMaxThreads(size_t count);

    /**
     * @brief setMaxFreeThreads Set thread count.
     * @param count Number of threads keep active without a job.
     */
    ThreadPoolOptions & setMaxFreeThreads(size_t count);

    /**
     * @brief setQueueSize Set single worker queue size.
     * @param count Maximum length of queue of single worker.
     */
    ThreadPoolOptions & setQueueSize(size_t count);

    /**
     * @brief threadCount Return thread count.
     */
    size_t threadCount() const;

    /**
     * @brief queueSize Return single worker queue size.
     */
    size_t queueSize() const;

    size_t maxFreeThreads() const;

private:
    size_t m_thread_count;
    ssize_t m_max_free_threads;
    size_t m_queue_size;
};

/// Implementation

inline ThreadPoolOptions::ThreadPoolOptions()
    : m_thread_count(std::max<size_t>(1u, std::thread::hardware_concurrency()))
    , m_max_free_threads(-1)
    , m_queue_size(1024u)
{
}

inline ThreadPoolOptions &  ThreadPoolOptions::setMaxThreads(size_t count)
{
    m_thread_count = std::max<size_t>(1u, count);
    return *this;
}

inline ThreadPoolOptions &  ThreadPoolOptions::setQueueSize(size_t size)
{
    m_queue_size = std::max<size_t>(1u, size);
    return *this;
}

inline ThreadPoolOptions &  ThreadPoolOptions::setMaxFreeThreads(size_t size)
{
    m_max_free_threads = std::max<size_t>(1u, size);
    return *this;
}

inline size_t ThreadPoolOptions::threadCount() const
{
    return m_thread_count;
}

inline size_t ThreadPoolOptions::queueSize() const
{
    return m_queue_size;
}

inline size_t ThreadPoolOptions::maxFreeThreads() const
{
    return m_max_free_threads == -1 ? 0 : m_max_free_threads;
}

}
