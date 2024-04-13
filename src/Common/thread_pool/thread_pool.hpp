#pragma once

#include "./fixed_function.hpp"
#include "./thread_pool_options.hpp"
#include "./worker.hpp"
#include "./handler.hpp"

#include <mutex>
#include <atomic>
#include <memory>
#include <stdexcept>
#include <vector>
#include <iostream>

#include <functional>
#include <stack>

#include <Common/noexcept_scope.h>

namespace tp
{


template <typename T>
struct atomwrapper
{
  std::atomic<T> _a;

  atomwrapper()
    :_a()
  {}

  atomwrapper(const std::atomic<T> &a)
    :_a(a.load())
  {}

  atomwrapper(const atomwrapper &other)
    :_a(other._a.load())
  {}

  atomwrapper &operator=(const atomwrapper &other)
  {
    _a.store(other._a.load());
  }

  T load()
  {
    return _a.load();
  }

  void store(T val)
  {
    return _a.store(val);
  }

};


template <typename Task>
class ThreadPoolImpl;
using ThreadPool = ThreadPoolImpl<FixedFunction<void(), 128>>;

/**
 * @brief The ThreadPool class implements thread pool pattern.
 * It is highly scalable and fast.
 * It is header only.
 * It implements both work-stealing and work-distribution balancing
 * startegies.
 * It implements cooperative scheduling strategy for tasks.
 */
template <typename Task>
class ThreadPoolImpl : public ActiveWorkers<Task> {
public:
    using ActiveWorkers<Task>::m_active_tasks;

    /**
     * @brief ThreadPool Construct and start new thread pool.
     * @param options Creation options.
     */
    explicit ThreadPoolImpl(
        const ThreadPoolOptions& options = ThreadPoolOptions());

    /**
     * @brief Move ctor implementation.
     */
    ThreadPoolImpl(ThreadPoolImpl&& rhs) noexcept;

    /**
     * @brief ~ThreadPool Stop all workers and destroy thread pool.
     */
    ~ThreadPoolImpl() override;

    /**
     * @brief Move assignment implementaion.
     */
    ThreadPoolImpl& operator=(ThreadPoolImpl&& rhs) noexcept;

    /**
     * @brief post Try post job to thread pool.
     * @param handler Handler to be called from thread pool worker. It has
     * to be callable as 'handler()'.
     * @return 'true' on success, false otherwise.
     * @note All exceptions thrown by handler will be suppressed.
     */
    template <typename Handler>
    bool tryPost(Handler&& handler);

    /**
     * @brief post Post job to thread pool.
     * @param handler Handler to be called from thread pool worker. It has
     * to be callable as 'handler()'.
     * @throw std::overflow_error if worker's queue is full.
     * @note All exceptions thrown by handler will be suppressed.
     */
    template <typename Handler>
    void post(Handler&& handler);

    template <typename Handler>
    void scheduleOrThrow(Handler&& handler);


    /**
     * @brief Wait for all currently active jobs to be done.
     * @note You may call schedule and wait many times in arbitrary order.
     * If any thread was throw an exception, first exception will be rethrown from this method,
     *  and exception will be cleared.
     */
    void wait();

    size_t getActiveThreads()
    {
        return m_num_workers;
    }

    void tryShrink(Worker<Task>*);

    bool steal(Task & task, size_t acceptor_num) override;


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

    Worker<Task>* getWorker();
    void onDestroy();

    ThreadPoolOptions m_options;
    std::atomic<size_t> m_num_workers;
    std::vector<std::unique_ptr<Worker<Task>>> m_workers;
    std::vector<atomwrapper<Worker<Task>*>> m_raw_workers;
    std::vector<std::unique_ptr<Worker<Task>>> m_orphaned_workers;
    std::vector<size_t> m_free_workers;
    std::atomic<size_t> m_next_worker;
    std::mutex m_mutex;
    std::atomic<size_t> m_shrink_attempt;
    std::stack<OnDestroyCallback> on_destroy_callbacks;
};


/// Implementation

template <typename Task>
inline ThreadPoolImpl<Task>::ThreadPoolImpl(const ThreadPoolOptions& options)
    : m_options(options)
    , m_num_workers(options.threadCount() - options.maxFreeThreads())
    , m_workers(options.threadCount())
    , m_raw_workers(options.threadCount())
    , m_orphaned_workers(options.threadCount())
    , m_next_worker(0)
{
    m_free_workers.reserve(options.threadCount());
    for(size_t i = 0; i < m_num_workers; ++i)
    // for(auto& worker_ptr : m_workers)
    {
        m_workers[i].reset(new Worker<Task>(options.queueSize(), this));
        m_raw_workers[i].store(m_workers[i].get());
    }

    for(size_t i = m_num_workers; i < m_workers.size(); ++i)
    {
        m_free_workers.push_back(i);
    }

    for(size_t i = 0; i < m_num_workers; ++i)
    {
        m_workers[i]->start(i);
    }
}

template <typename Task>
inline ThreadPoolImpl<Task>::ThreadPoolImpl(ThreadPoolImpl<Task>&& rhs) noexcept
{
    std::unique_lock lock(m_mutex);
    *this = rhs;
}

template <typename Task>
inline ThreadPoolImpl<Task>::~ThreadPoolImpl()
{
    try
    {
        wait();
    }
    catch(...)
    {
    }
}

template <typename Task>
inline bool ThreadPoolImpl<Task>::steal(Task & task, size_t acceptor_num)
{
    // precheck if small number of tasks
    for (size_t attempt = 1; attempt <= 2; ++ attempt)
    {
        auto donor_num = (acceptor_num + attempt) % m_num_workers;
        auto ptr = m_raw_workers[donor_num].load();
        if (ptr)
        {
            auto ret = ptr->steal(task);
            if (ret)
            {
                return true;
            }
        }
    }
    return false;
}


template <typename Task>
inline void ThreadPoolImpl<Task>::wait()
{
    std::unique_lock lock(m_mutex);
    size_t num = 0;
    for (auto & worker_ptr : m_raw_workers)
    {
        auto ptr = worker_ptr.load();
        if (ptr)
        {
            ptr->stop();
            m_raw_workers[num].store(nullptr);
            m_orphaned_workers[num] = std::move(m_workers[num]);
            m_num_workers--;
        }
        ++num;
    }
}

template <typename Task>
inline ThreadPoolImpl<Task>&
ThreadPoolImpl<Task>::operator=(ThreadPoolImpl<Task>&& rhs) noexcept
{
    if (this != &rhs)
    {
        std::unique_lock lock(m_mutex);

        m_workers = std::move(rhs.m_workers);
        m_raw_workers = std::move(rhs.m_raw_workers);
        m_orphaned_workers = std::move(rhs.m_orphaned_workers);
        m_free_workers = std::move(rhs.m_free_workers);

        m_next_worker = rhs.m_next_worker.load();
        m_num_workers = rhs.m_num_workers.load();
    }
    return *this;
}

template <typename Task>
template <typename Handler>
inline bool ThreadPoolImpl<Task>::tryPost(Handler&& handler)
{
    auto worker = getWorker();
    bool try_shrink = false;

    if (worker->is_busy())
    {
        // std::cout << "getWorker().is_busy()" << std::endl;

        size_t worker_id = 0;
        for (; worker_id < m_workers.size(); ++worker_id)
        {
            if (m_workers[worker_id] && !m_workers[worker_id]->is_busy())
            {
                // Worker<Task>::setWorkerIdForCurrentThread(worker_id);
                worker = m_workers[worker_id].get();
                break;
            }
        }
        if (worker_id == m_workers.size())
        {
            std::unique_lock lock(m_mutex, std::defer_lock);
            if (lock.try_lock())
            {
                while (m_num_workers < m_options.threadCount()) /// empty slots?
                {
                    size_t new_worker_num = 0;

                    new_worker_num = m_workers.size();
                    // std::cout << "m_active_tasks " << m_active_tasks << " < new_worker_num " <<  new_worker_num << ", m_options.threadCount() " << m_options.threadCount() << std::endl;
                    if (m_active_tasks < new_worker_num * 1 || new_worker_num >= m_options.threadCount())
                    {
                        // no courage to create new workers
                        break;
                    }

                    assert(!m_free_workers.empty());  // need atomic check !

                    new_worker_num = m_free_workers.back();
                    m_free_workers.pop_back();
                    m_workers[new_worker_num] = std::move(std::make_unique<Worker<Task>>(m_options.queueSize(), this));
                    lock.unlock();

                    worker = m_workers[new_worker_num].get();

                    worker->start(new_worker_num);
                    break;
                }
            }
        }
    }
    else
    {
        try_shrink = true;
    }

    const auto & post_ret = worker->post(std::forward<Handler>(handler));
    if (try_shrink)
    {
        tryShrink(worker);
    }
    return post_ret;
}


template <typename Task>
inline void ThreadPoolImpl<Task>::tryShrink(Worker<Task>* /* worker */)
{
    // std::cout << "Top of tryShrink()" << std::endl;

    if (!(m_shrink_attempt++ % skip_shrink_attempts))
    {
        std::unique_lock lock(m_mutex, std::defer_lock);
        if (lock.try_lock())
        {
            for (auto & wrk : m_orphaned_workers)
            {
                if (wrk && !wrk->is_busy())
                {
                    wrk->stop();
                    wrk.reset();
                }
            }

            auto now = std::chrono::steady_clock::now();
            for (size_t worker_num = 0; worker_num < m_workers.size() && m_num_workers > m_options.threadCount() - m_options.maxFreeThreads(); ++worker_num)
            {
                const auto worker_ptr = m_workers[worker_num].get();
                if (worker_ptr && !worker_ptr->is_busy())
                {
                    auto idle_since = worker_ptr->idleSince();
                    if (std::chrono::duration_cast<std::chrono::milliseconds>(now - idle_since).count() > idle_milliseconds)
                    {
                        // std::cout << "shrinking" << std::endl;
                        worker_ptr->stop();
                        auto num = worker_ptr->get_id();
                        m_raw_workers[num].store(nullptr);
                        m_orphaned_workers[num] = std::move(m_workers[num]);
                        m_free_workers.push_back(num);
                        m_num_workers--;
                    }
                }
            }
        }
    }
}


template <typename Task>
template <typename Handler>
inline void ThreadPoolImpl<Task>::post(Handler&& handler)
{
    const auto ok = tryPost(std::forward<Handler>(handler));
    if (!ok)
    {
        throw std::runtime_error("thread pool queue is full");
    }
}

template <typename Task>
template <typename Handler>
inline void ThreadPoolImpl<Task>::scheduleOrThrow(Handler&& handler)
{
    const auto ok = tryPost(std::forward<Handler>(handler));
    if (!ok)
    {
        throw std::runtime_error("thread pool queue is full");
    }
}

template <typename Task>
inline Worker<Task>* ThreadPoolImpl<Task>::getWorker()
{
    auto id = Worker<Task>::getWorkerIdForCurrentThread();

    Worker<Task>* raw_ptr = nullptr;
    for (; !raw_ptr; id = m_next_worker.fetch_add(1, std::memory_order_relaxed) % m_workers.size())
    {
        if (id < m_workers.size())
        {
            raw_ptr = m_raw_workers[id].load();
        }
    }


    // std::cerr << id << ", " << std::this_thread::get_id() << std::endl;

    return raw_ptr;
}

template <typename Task>
inline void ThreadPoolImpl<Task>::addOnDestroyCallback(OnDestroyCallback && callback)
{
    std::lock_guard lock(m_mutex);
    on_destroy_callbacks.push(std::move(callback));
}

template <typename Task>
inline void ThreadPoolImpl<Task>::onDestroy()
{
    while (!on_destroy_callbacks.empty())
    {
        auto callback = std::move(on_destroy_callbacks.top());
        on_destroy_callbacks.pop();
        NOEXCEPT_SCOPE({ callback(); });
    }
}
}
