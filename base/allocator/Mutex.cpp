#include <allocator/Mutex.h>

#include <allocator/Format.h>
#include <allocator/Spin.h>

#include <cstdlib>

#if defined(__FreeBSD__)
/// libc's hook to initialize a mutex without recursing into malloc.
extern "C" int _pthread_mutex_init_calloc_cb(pthread_mutex_t * mutex, void * (*calloc_callback)(size_t, size_t));
#endif

namespace jemalloc
{

constinit bool is_threaded = false;

#if defined(__FreeBSD__)
namespace
{

/// With `JEMALLOC_MUTEX_INIT_CB`: mutexes initialized before `malloc_mutex_boot` are put on a list and initialized
/// there.
constinit bool postpone_init = true;
constinit Mutex * postponed_mutexes = nullptr;

}
#endif

void Mutex::lockSlow()
{
    MutexProfilingData & data = profiling_data;

    /// jemalloc jumps to `label_spin_done` when `num_cpus == 1`.
    if (num_cpus != 1)
    {
        int count = 0;
        do
        {
            spinCPUSpinWait();
            if (!locked.load(std::memory_order_relaxed) && !tryLockFinal())
            {
                ++data.num_spin_acquired;
                return;
            }
        } while (count++ < options.mutex_max_spin || options.mutex_max_spin == -1);

        if constexpr (!config::stats)
        {
            /// Only spin is useful when stats is off.
            lockFinal();
            return;
        }
    }

    /// label_spin_done:
    Nanoseconds before;
    before.initUpdate();
    /// Copy before to after to avoid clock skews.
    Nanoseconds after;
    after.copy(before);
    uint32_t num_threads = data.num_waiting_threads.fetch_add(1, std::memory_order_relaxed) + 1;
    /// One last try as above two calls may take quite some cycles.
    if (!tryLockFinal())
    {
        data.num_waiting_threads.fetch_sub(1, std::memory_order_relaxed);
        ++data.num_spin_acquired;
        return;
    }

    /// True slow path.
    lockFinal();
    /// Update more slow-path only counters.
    data.num_waiting_threads.fetch_sub(1, std::memory_order_relaxed);
    after.update();

    Nanoseconds delta;
    delta.copy(after);
    delta.subtract(before);

    ++data.num_wait_times;
    data.total_wait_time.add(delta);
    if (data.max_wait_time.compare(delta) < 0)
        data.max_wait_time.copy(delta);
    if (num_threads > data.max_num_threads)
        data.max_num_threads = num_threads;
}

void Mutex::profilingDataReset(ThreadState * thread_state)
{
    assertOwner(thread_state);
    profiling_data.reset();
}

bool Mutex::init(const char * /*name*/, MutexRank /*rank*/, MutexLockOrder /*lock_order*/)
{
    profiling_data.reset();

#if defined(__FreeBSD__)
    if (postpone_init)
    {
        postponed_next = postponed_mutexes;
        postponed_mutexes = this;
    }
    else
    {
        if (_pthread_mutex_init_calloc_cb(&lock_, bootstrapAllocateZeroed) != 0)
            return true;
    }
#else
    pthread_mutexattr_t attributes;
    if (pthread_mutexattr_init(&attributes) != 0)
        return true;
    /// MALLOC_MUTEX_TYPE
    pthread_mutexattr_settype(&attributes, PTHREAD_MUTEX_DEFAULT);
    if (pthread_mutex_init(&lock_, &attributes) != 0)
    {
        pthread_mutexattr_destroy(&attributes);
        return true;
    }
    pthread_mutexattr_destroy(&attributes);
#endif

    /// Witness (`config_debug` only) is not implemented.
    return false;
}

void Mutex::prefork(ThreadState * thread_state)
{
    lock(thread_state);
}

void Mutex::postforkParent(ThreadState * thread_state)
{
    unlock(thread_state);
}

void Mutex::postforkChild(ThreadState * thread_state)
{
    if constexpr (config::mutex_init_callback)
    {
        unlock(thread_state);
    }
    else
    {
        /// jemalloc passes the witness name/rank/lock order, which are only meaningful in debug builds.
        if (init("mutex", MutexRank::OMIT, MutexLockOrder::RankExclusive))
        {
            printMessage("<jemalloc>: Error re-initializing mutex in child\n");
            if (options.abort)
                abort();
        }
    }
}

bool Mutex::boot()
{
#if defined(__FreeBSD__)
    postpone_init = false;
    while (postponed_mutexes != nullptr)
    {
        if (_pthread_mutex_init_calloc_cb(&postponed_mutexes->lock_, bootstrapAllocateZeroed) != 0)
            return true;
        postponed_mutexes = postponed_mutexes->postponed_next;
    }
#endif
    return false;
}

}
