#pragma once

// #include "./fixed_function.hpp"
#include "./thread_pool_options.hpp"
// #include "./worker.hpp"
#include "./handler.hpp"


#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>

#include <functional>
#include <stack>

#include <Common/noexcept_scope.h>
#include <Common/CurrentMetrics.h>

namespace tp
{

template <typename Task>
class Worker;


template <typename T>
struct atomwrapper
{
    std::atomic<T> _a;

    atomwrapper() : _a() { }

    atomwrapper(const std::atomic<T> & a) : _a(a.load()) { }

    atomwrapper(const atomwrapper & other) : _a(other._a.load()) { }

    atomwrapper & operator=(const atomwrapper & other) { _a.store(other._a.load()); }

    T load() { return _a.load(); }

    void store(T val) { return _a.store(val); }
};


template <typename Task>
class ThreadPoolImpl;
// using ThreadPool = ThreadPoolImpl<FixedFunction<void(), 128>>;
using ThreadPool = ThreadPoolImpl<std::function<void()>>;

/**
 * @brief The ThreadPool class implements thread pool pattern.
 * It is highly scalable and fast.
 * It is header only.
 * It implements both work-stealing and work-distribution balancing
 * strategies.
 * It implements cooperative scheduling strategy for tasks.
 */
template <typename Task>
class ThreadPoolImpl : public ActiveWorkers<Task>
{
public:
    using ActiveWorkers<Task>::m_scheduled_jobs;
    using Job = std::function<void()>;
    using Metric = CurrentMetrics::Metric;

    /**
     * @brief ThreadPool Construct and start new thread pool.
     * @param options Creation options.
     */
    explicit ThreadPoolImpl(Metric metric_threads_,
        Metric metric_active_threads_,
        Metric metric_scheduled_jobs_,
        const ThreadPoolOptions & options = ThreadPoolOptions());

    // /**
    //  * @brief Move ctor implementation.
    //  */
    // ThreadPoolImpl(ThreadPoolImpl && rhs) noexcept;

    /**
     * @brief ~ThreadPool Stop all workers and destroy thread pool.
     */
    ~ThreadPoolImpl() override;

    /**
     * @brief post Try post job to thread pool.
     * @param handler Handler to be called from thread pool worker. It has
     * to be callable as 'handler()'.
     * @return 'true' on success, false otherwise.
     * @note All exceptions thrown by handler will be suppressed.
     */
    // template <typename Handler>
    bool tryPost(Job && handler);

    /**
     * @brief post Post job to thread pool.
     * @param handler Handler to be called from thread pool worker. It has
     * to be callable as 'handler()'.
     * @throw std::overflow_error if worker's queue is full.
     * @note All exceptions thrown by handler will be suppressed.
     */
    // template <typename Handler>
    void post(Job && handler);

    // template <typename Handler>
    void scheduleOrThrow(Job && handler);


    /**
     * @brief Wait for all currently active jobs to be done.
     * @note You may call schedule and wait many times in arbitrary order.
     * If any thread was throw an exception, first exception will be rethrown from this method,
     *  and exception will be cleared.
     */
    void wait();

    void finalize();

    size_t getActiveThreads() { return m_num_workers; }

    void tryShrink(Worker<Task> *);

    bool steal(Task & task, size_t acceptor_num);


    /// Adds a callback which is called in destructor after
    /// joining of all threads. The order of calling callbacks
    /// is reversed to the order of their addition.
    /// It may be useful for static thread pools to call
    /// function after joining of threads because order
    /// of destructors of global static objects and callbacks
    /// added by atexit is undefined for different translation units.
    using OnDestroyCallback = std::function<void()>;
    void addOnDestroyCallback(OnDestroyCallback && callback);

private:
    const size_t skip_shrink_attempts = 1; // 3
    const long idle_milliseconds = 50; // 1000

    Worker<Task> * getWorker();
    void onDestroy();

    ThreadPoolOptions m_options;
    std::atomic<size_t> m_num_workers;
    std::vector<std::unique_ptr<Worker<Task>>> m_workers;
    std::vector<atomwrapper<Worker<Task> *>> m_raw_workers;
    std::vector<std::unique_ptr<Worker<Task>>> m_orphaned_workers;
    std::vector<size_t> m_free_workers;
    std::atomic<size_t> m_next_worker;
    std::mutex m_mutex;
    std::atomic<size_t> m_shrink_attempt;
    std::stack<OnDestroyCallback> on_destroy_callbacks;

    Metric metric_threads;
};


}
