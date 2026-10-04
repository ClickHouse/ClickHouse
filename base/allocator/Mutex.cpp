#include <allocator/Mutex.h>

#include <allocator/Format.h>
#include <allocator/Spin.h>

#include <cstdlib>

#if defined(__FreeBSD__)
/// libc's hook to initialize a mutex without recursing into malloc.
extern "C" int _pthread_mutex_init_calloc_cb(pthread_mutex_t * mutex, void * (*calloc_cb)(size_t, size_t));
#endif

namespace jemalloc
{

constinit bool isthreaded = false;

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
    MutexProfData & data = prof_data;

    /// jemalloc jumps to `label_spin_done` when `ncpus == 1`.
    if (ncpus != 1)
    {
        int cnt = 0;
        do
        {
            spinCPUSpinwait();
            if (!locked.load(std::memory_order_relaxed) && !trylockFinal())
            {
                ++data.n_spin_acquired;
                return;
            }
        } while (cnt++ < opt.mutex_max_spin || opt.mutex_max_spin == -1);

        if constexpr (!config::stats)
        {
            /// Only spin is useful when stats is off.
            lockFinal();
            return;
        }
    }

    /// label_spin_done:
    NsTime before;
    before.initUpdate();
    /// Copy before to after to avoid clock skews.
    NsTime after;
    after.copy(before);
    uint32_t n_thds = data.n_waiting_thds.fetch_add(1, std::memory_order_relaxed) + 1;
    /// One last try as above two calls may take quite some cycles.
    if (!trylockFinal())
    {
        data.n_waiting_thds.fetch_sub(1, std::memory_order_relaxed);
        ++data.n_spin_acquired;
        return;
    }

    /// True slow path.
    lockFinal();
    /// Update more slow-path only counters.
    data.n_waiting_thds.fetch_sub(1, std::memory_order_relaxed);
    after.update();

    NsTime delta;
    delta.copy(after);
    delta.subtract(before);

    ++data.n_wait_times;
    data.tot_wait_time.add(delta);
    if (data.max_wait_time.compare(delta) < 0)
        data.max_wait_time.copy(delta);
    if (n_thds > data.max_n_thds)
        data.max_n_thds = n_thds;
}

void Mutex::profDataReset(ThreadState * tsdn)
{
    assertOwner(tsdn);
    prof_data.reset();
}

bool Mutex::init(const char * /*name*/, MutexRank /*rank*/, MutexLockOrder /*lock_order*/)
{
    prof_data.reset();

#if defined(__FreeBSD__)
    if (postpone_init)
    {
        postponed_next = postponed_mutexes;
        postponed_mutexes = this;
    }
    else
    {
        if (_pthread_mutex_init_calloc_cb(&lock_, bootstrapCalloc) != 0)
            return true;
    }
#else
    pthread_mutexattr_t attr;
    if (pthread_mutexattr_init(&attr) != 0)
        return true;
    /// MALLOC_MUTEX_TYPE
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_DEFAULT);
    if (pthread_mutex_init(&lock_, &attr) != 0)
    {
        pthread_mutexattr_destroy(&attr);
        return true;
    }
    pthread_mutexattr_destroy(&attr);
#endif

    /// Witness (`config_debug` only) is not implemented.
    return false;
}

void Mutex::prefork(ThreadState * tsdn)
{
    lock(tsdn);
}

void Mutex::postforkParent(ThreadState * tsdn)
{
    unlock(tsdn);
}

void Mutex::postforkChild(ThreadState * tsdn)
{
    if constexpr (config::mutex_init_cb)
    {
        unlock(tsdn);
    }
    else
    {
        /// jemalloc passes the witness name/rank/lock order, which are only meaningful in debug builds.
        if (init("mutex", MutexRank::OMIT, MutexLockOrder::RankExclusive))
        {
            printMessage("<jemalloc>: Error re-initializing mutex in child\n");
            if (opt.abort)
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
        if (_pthread_mutex_init_calloc_cb(&postponed_mutexes->lock_, bootstrapCalloc) != 0)
            return true;
        postponed_mutexes = postponed_mutexes->postponed_next;
    }
#endif
    return false;
}

}
