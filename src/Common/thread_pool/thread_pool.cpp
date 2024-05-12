#include "./thread_pool.hpp"
#include "./worker.hpp"
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
    extern const Event GlobalThreadPoolExpansions;
    extern const Event GlobalThreadPoolShrinks;
    extern const Event GlobalThreadPoolJobScheduleMicroseconds;
}


namespace tp
{

template <typename Task>
inline ThreadPoolImpl<Task>::ThreadPoolImpl(Metric metric_threads_,
    Metric metric_active_threads_,
    Metric metric_scheduled_jobs_,
    const ThreadPoolOptions & options)
    : ActiveWorkers<Task>(metric_active_threads_, metric_scheduled_jobs_)
    , m_options(options)
    // , m_num_workers(options.threadCount() - options.maxFreeThreads())
    , m_num_workers(std::max(2UL, options.maxFreeThreads()))
    , m_workers(options.threadCount())
    , m_raw_workers(options.threadCount())
    , m_orphaned_workers(options.threadCount())
    , m_next_worker(0)
    , metric_threads(metric_threads_)
{
    m_free_workers.reserve(options.threadCount());
    for (size_t i = 0; i < m_num_workers; ++i)
    // for(auto& worker_ptr : m_workers)
    {
        m_workers[i].reset(new Worker<Task>(options.queueSize(), this));
        m_raw_workers[i].store(m_workers[i].get());
    }

    for (size_t i = m_num_workers; i < m_workers.size(); ++i)
        m_free_workers.push_back(i);

    for (size_t i = 0; i < m_num_workers; ++i)
        m_workers[i]->start(i, [this](Task & task, size_t acceptor_num) { return this->steal(task, acceptor_num); });

    metric_threads_ = m_num_workers;
}

// template <typename Task>
// inline ThreadPoolImpl<Task>::ThreadPoolImpl(ThreadPoolImpl<Task> && rhs) noexcept
// {
//     std::unique_lock lock(m_mutex);
//     *this = std::move(rhs);
// }

template <typename Task>
inline ThreadPoolImpl<Task>::~ThreadPoolImpl()
{
    try
    {
        finalize();
    }
    catch (...)
    {
    }
}

template <typename Task>
inline bool ThreadPoolImpl<Task>::steal(Task & task, size_t acceptor_num)
{
    // precheck if small number of tasks
    for (size_t attempt = 1; attempt <= 2; ++attempt)
    {
        auto donor_num = (acceptor_num + attempt) % m_num_workers;
        auto ptr = m_raw_workers[donor_num].load();
        if (ptr)
        {
            auto ret = ptr->steal(task);
            if (ret)
                return true;
        }
    }
    return false;
}


template <typename Task>
inline void ThreadPoolImpl<Task>::finalize()
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
            metric_threads--;
        }
        ++num;
    }
}

template <typename Task>
inline void ThreadPoolImpl<Task>::wait()
{
    while (m_scheduled_jobs)
    {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1s);
    }
}

template <typename Task>
// template <typename Handler>
inline bool ThreadPoolImpl<Task>::tryPost(Job && handler)
{
    Stopwatch watch;

    auto worker = getWorker();
    bool try_shrink = false;

    if (worker->make_busy())
    {
        // std::cout << "getWorker().is_busy()" << std::endl;

        size_t worker_id = 0;
        for (; worker_id < m_workers.size(); ++worker_id)
        {
            if (auto raw_worker = m_raw_workers[worker_id].load(); raw_worker && !raw_worker->make_busy())  // max_active_worker for optimization ?
            {
                // Worker<Task>::setWorkerIdForCurrentThread(worker_id);
                worker = raw_worker;
                break;
            }
        }
        if (worker_id == m_workers.size())
        {
            std::unique_lock lock(m_mutex);
            while (m_num_workers < m_options.threadCount() /*m_workers.size() */)
            {
                size_t new_worker_num = 0;

                // if (m_scheduled_jobs < m_num_workers * 1)
                // {
                //     // no courage to create new workers
                //     break;
                // }

                assert(!m_free_workers.empty()); // need atomic check !

                new_worker_num = m_free_workers.back();
                m_free_workers.pop_back();
                assert(!m_workers[new_worker_num]);
                m_workers[new_worker_num].reset(new Worker<Task>(m_options.queueSize(), this));

                worker = m_workers[new_worker_num].get();
                m_num_workers++;
                lock.unlock();

                worker->start(new_worker_num, [this](Task & task, size_t acceptor_num) { return this->steal(task, acceptor_num); });
                m_raw_workers[new_worker_num].store(worker);
                ProfileEvents::increment(ProfileEvents::GlobalThreadPoolExpansions);

                break;
            }
        }
    }
    else
    {
        try_shrink = true;
    }

    ProfileEvents::increment(ProfileEvents::GlobalThreadPoolJobScheduleMicroseconds, watch.elapsedNanoseconds());

    // ProfileEvents::global_counters.increment(ProfileEvents::GlobalThreadPoolJobScheduleMicroseconds, watch.elapsedMicroseconds());


    // std::cout << ProfileEvents::global_counters[ProfileEvents::GlobalThreadPoolJobScheduleMicroseconds] << " ";


    const auto & post_ret = worker->post(std::forward<Job>(handler));
    if (try_shrink)
        tryShrink(worker);
    return post_ret;
}


template <typename Task>
inline void ThreadPoolImpl<Task>::tryShrink(Worker<Task> * /* worker */)
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
            auto max_free_threads = std::max(2UL, m_options.maxFreeThreads());
            for (size_t worker_num = 0; worker_num < m_workers.size() && m_num_workers > max_free_threads; ++worker_num)
            {
                const auto worker_ptr = m_raw_workers[worker_num].load();
                if (worker_ptr && !worker_ptr->is_busy())
                {
                    auto idle_since = worker_ptr->idleSince();
                    if (std::chrono::duration_cast<std::chrono::milliseconds>(now - idle_since).count() > idle_milliseconds)
                    {
                        auto num = worker_ptr->get_id();
                        m_raw_workers[num].store(nullptr);
                        m_orphaned_workers[num] = std::move(m_workers[num]);
                        m_free_workers.push_back(num);
                        m_num_workers--;
                        ProfileEvents::increment(ProfileEvents::GlobalThreadPoolShrinks);
                    }
                }
            }
        }
    }
}


template <typename Task>
// template <typename Handler>
inline void ThreadPoolImpl<Task>::post(Job && handler)
{
    const auto ok = tryPost(std::forward<Job>(handler));
    if (!ok)
        throw std::runtime_error("thread pool queue is full");
}

template <typename Task>
// template <typename Handler>
inline void ThreadPoolImpl<Task>::scheduleOrThrow(Job && handler)
{
    const auto ok = tryPost(std::forward<Job>(handler));
    if (!ok)
        throw std::runtime_error("thread pool queue is full");
}

template <typename Task>
inline Worker<Task> * ThreadPoolImpl<Task>::getWorker()
{
    auto id = Worker<Task>::getWorkerIdForCurrentThread();

    Worker<Task> * raw_ptr = nullptr;
    for (; !raw_ptr; id = m_next_worker.fetch_add(1, std::memory_order_relaxed) % m_workers.size())
        if (id < m_workers.size())
            raw_ptr = m_raw_workers[id].load();


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

// template class tp::ThreadPoolImpl<std::thread>;
// template class tp::ThreadPoolImpl<ThreadFromGlobalPoolImpl<false>>;
// template class ThreadFromGlobalPoolImpl<true>;

// template class tp::ThreadPoolImpl<tp::FixedFunction<void(), 128>>;
template class tp::ThreadPoolImpl<std::function<void()>>;
