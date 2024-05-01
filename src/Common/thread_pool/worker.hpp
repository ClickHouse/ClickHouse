#pragma once

#include <atomic>
#include <thread>
// #include <semaphore>
#include <condition_variable>
#include <mutex>
#include <boost/circular_buffer.hpp>

#include <iostream>
#include "./handler.hpp"

namespace tp
{

/**
 * @brief The Worker class owns task queue and executing thread.
 * In thread it tries to poqp task from queue. If queue is empty then it tries
 * to steal task from the sibling worker. If steal was unsuccessful then waits
 */
template <typename Task>
class Worker
{
public:
    /**
     * @brief Worker Constructor.
     * @param queue_size Length of undelaying task queue.
     */
    explicit Worker(size_t queue_size, ActiveWorkers<Task> * handler_ptr);

    /**
     * @brief Move ctor implementation.
     */
    Worker(Worker && rhs) noexcept;

    /**
     * @brief Move assignment implementaion.
     */
    Worker & operator=(Worker && rhs) noexcept;

    /**
     * @brief start Create the executing thread and start tasks execution.
     * @param id Worker ID.
     */
    void start(size_t id, std::function<bool(Task &, size_t)> parent_steal_);

    /**
     * @brief stop Stop all worker's thread and stealing activity.
     * Waits until the executing thread became finished.
     */
    void stop();

    /**
     * @brief post Post task to queue.
     * @param handler Handler to be executed in executing thread.
     * @return true on success.
     */
    template <typename Handler>
    bool post(Handler && handler);


    /**
     * @brief getWorkerIdForCurrentThread Return worker ID associated with
     * current thread if exists.
     * @return Worker ID.
     */
    static size_t getWorkerIdForCurrentThread();

    static void setWorkerIdForCurrentThread(size_t id);


    bool is_busy() const
    {
        if (m_busy)
        {
            // std::cout << (void*)this << " is_busy about to return true" << std::endl;
            return true;
        }
        else
        {
            // std::cout << (void*)this << "is_busy about to return false" << std::endl;
            return false;
        }
    }

    ssize_t get_id() { return m_id; }

    std::chrono::time_point<std::chrono::steady_clock> idleSince() { return m_idle_since; }

    bool steal(Task & task);

private:
    /**
     * @brief threadFunc Executing thread function.
     * @param id Worker ID to be associated with this thread.
     */
    void threadFunc(size_t id);

    template <typename T>
    bool pop(T & val)
    {
        if (m_cb.empty())
        {
            // m_busy = false;
            return false;
        }
        else
        {
            val = std::move(m_cb.front());
            m_cb.pop_front();
            return true;
        }
    }

    // Queue<Task> m_queue;
    boost::circular_buffer<Task> m_cb;

    std::atomic<bool> m_running_flag;
    std::thread m_thread;
    std::mutex m_mutex;
    std::condition_variable m_cond_var;

    // bool m_busy;

    ActiveWorkers<Task> * m_handler_ptr;
    std::atomic<bool> m_busy;
    ssize_t m_id = -1;

    std::chrono::time_point<std::chrono::steady_clock> m_idle_since;
    std::function<bool(Task &, size_t)> parent_steal;
};


/// Implementation

namespace detail
{
inline size_t * thread_id()
{
    static thread_local size_t tss_id = -1u;
    return &tss_id;
}
}

template <typename Task>
inline Worker<Task>::Worker(size_t queue_size, ActiveWorkers<Task> * handler_ptr)
    : m_cb(queue_size), m_running_flag(true), m_handler_ptr(handler_ptr)
// , m_busy(false)
{
}

template <typename Task>
inline Worker<Task>::Worker(Worker && rhs) noexcept
{
    *this = rhs;
}

template <typename Task>
inline Worker<Task> & Worker<Task>::operator=(Worker && rhs) noexcept
{
    if (this != &rhs)
    {
        m_cb = std::move(rhs.m_cb);
        m_running_flag = rhs.m_running_flag.load();
        m_handler_ptr->m_active_tasks = rhs.m_handler_ptr->m_active_workers.load();
        m_thread = std::move(rhs.m_thread);
        m_busy = rhs.m_busy.load();
    }
    return *this;
}

template <typename Task>
inline void Worker<Task>::stop()
{
    // std::cout << std::this_thread::get_id() << " m_id=" << m_id << " Worker::stop()" << std::endl;
    m_running_flag.store(false, std::memory_order_relaxed);
    // m_sema.release();
    m_cond_var.notify_all();
    m_thread.join();
}

template <typename Task>
inline void Worker<Task>::start(size_t id, std::function<bool(Task &, size_t)> parent_steal_)
{
    m_id = id;

    assert(!m_thread.joinable());
    parent_steal = std::move(parent_steal_);

    m_thread = std::thread(&Worker<Task>::threadFunc, this, id);
}

template <typename Task>
inline size_t Worker<Task>::getWorkerIdForCurrentThread()
{
    size_t id = *detail::thread_id();
    // std::cout << std::this_thread::get_id() << " id=" << id << std::endl;
    return id;
}

template <typename Task>
inline void Worker<Task>::setWorkerIdForCurrentThread(size_t id)
{
    *detail::thread_id() = id;
}

template <typename Task>
template <typename Handler>
inline bool Worker<Task>::post(Handler && handler)
{
    bool ret = true;
    {
        ++m_handler_ptr->m_active_tasks;
        m_busy = true;
        // std::cout << (void*)this << " m_busy to true" << std::endl;
        std::unique_lock lock(m_mutex);
        // m_busy.store(true, std::memory_order_relaxed);
        m_cb.push_back(std::forward<Handler>(handler));
    }

    // m_sema.release();
    m_cond_var.notify_one();


    return ret;
}

template <typename Task>
inline bool Worker<Task>::steal(Task & task)
{
    std::lock_guard lock(m_mutex);
    if (pop(task))
    {
        --m_handler_ptr->m_active_tasks;
        return true;
    }
    // m_busy ??
    return false;
}

template <typename Task>
inline void Worker<Task>::threadFunc(size_t id)
{
    *detail::thread_id() = id;

    Task handler;

    while (m_running_flag.load(std::memory_order_relaxed))
    {
        bool got_task = false;
        {
            std::unique_lock lock(m_mutex);
            got_task = pop(handler);
        }


        if (got_task || (parent_steal(handler, m_id)))
        {
            // lock.unlock();  // too late in case of steal
            try
            {
                if (!m_busy)
                {
                    m_busy = true;
                    // ++m_handler_ptr->m_active_tasks;
                }

                // m_active = true;
                handler();
            }
            catch (...)
            {
                // suppress all exceptions
            }
            // m_active = false;
        }
        // else if (steal_donor->steal(handler))
        // {
        //     lock.unlock();  // too late in case of steal
        //     try
        //     {
        //         if (!m_busy)
        //         {
        //             m_busy = true;
        //             ++m_handler_ptr->m_active_tasks;
        //         }

        //         // m_active = true;
        //         handler();
        //     }
        //     catch(...)
        //     {
        //         // suppress all exceptions
        //     }
        // }
        else
        {
            if (m_busy)
            {
                // std::cout << (void*)this << " m_busy to false" << std::endl;
                m_busy = false;
                --m_handler_ptr->m_active_tasks;
            }
            m_idle_since = std::chrono::steady_clock::now();

            // std::this_thread::sleep_for(std::chrono::milliseconds(1));
            // m_sema.acquire();
            // m_busy.store(false, std::memory_order_relaxed);
            std::unique_lock lock(m_mutex);
            m_cond_var.wait(lock);
        }
    }
}

}
