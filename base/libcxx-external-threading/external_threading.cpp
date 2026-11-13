#include <SilkSuspensionBlocker.h>
#include <StdMutexHooks.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/platform.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <ctime>
#include <mutex>

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

_LIBCPP_BEGIN_NAMESPACE_STD

struct __clickhouse_condvar_waiter
{
    silk::FiberFuture future;
    uint32_t notified = 0;
    uint32_t claim = 0;
    bool fiber = false;
    __clickhouse_condvar_waiter * prev = nullptr;
    __clickhouse_condvar_waiter * next = nullptr;
    bool queued = false;
};

_LIBCPP_END_NAMESPACE_STD

namespace
{

bool needSilkAwareWait()
{
    return silk::FiberScheduler::getCurrentFiberId().raw != 0 && !Silk::SuspensionBlocker::isActive();
}

int futexWait(uint32_t & word, uint32_t expected, uint64_t deadline)
{
    const timespec timeout{.tv_sec = static_cast<time_t>(deadline / 1'000'000'000), .tv_nsec = static_cast<long>(deadline % 1'000'000'000)};
    if (::syscall(SYS_futex, &word, FUTEX_WAIT_BITSET_PRIVATE, expected, deadline == UINT64_MAX ? nullptr : &timeout, nullptr, FUTEX_BITSET_MATCH_ANY)
        && errno == ETIMEDOUT)
        return ETIMEDOUT;
    return 0;
}

void futexWake(uint32_t * word, int count)
{
    ::syscall(SYS_futex, word, FUTEX_WAKE_PRIVATE, count, nullptr, nullptr, 0);
}

class Mutex
{
public:
    explicit Mutex(std::__libcpp_mutex_t & storage_)
        : storage(storage_)
        , state(storage_.__state)
    {
    }

    void lock()
    {
        uint32_t current = 0;
        if (!state.compare_exchange_weak(current, LOCKED, std::memory_order_acquire, std::memory_order_relaxed))
            lockSlow();
    }

    bool tryLock()
    {
        uint32_t current = 0;
        return state.compare_exchange_strong(current, LOCKED, std::memory_order_acquire, std::memory_order_relaxed);
    }

    void unlock()
    {
        const uint32_t previous = state.exchange(0, std::memory_order_release);
        if (previous & THREAD_WAITERS)
            futexWake(&storage.__state, 1);
        if (previous & FIBER_WAITERS)
            silk::FiberScheduler::releaseWaiters(getWaiterKey());
    }

private:
    static constexpr uint32_t LOCKED = 1 << 0;
    static constexpr uint32_t THREAD_WAITERS = 1 << 1;
    static constexpr uint32_t FIBER_WAITERS = 1 << 2;

    void lockSlow()
    {
        const bool fiber = needSilkAwareWait();
        const uint32_t contended = LOCKED | (fiber ? FIBER_WAITERS : THREAD_WAITERS);
        uint32_t previous = state.fetch_or(contended, std::memory_order_acquire);
        while (previous & LOCKED)
        {
            if (fiber)
                silk::FiberScheduler::suspend(suspendCallback, this);
            else
                futexWait(storage.__state, previous | contended, UINT64_MAX);
            previous = state.fetch_or(contended, std::memory_order_acquire);
        }
    }

    static void suspendCallback(silk::Fiber * fiber, void * context) noexcept
    {
        auto & mutex = *static_cast<Mutex *>(context);
        silk::FiberScheduler::enqueueWaiter(mutex.getWaiterKey(), fiber);
        /// If FIBER_WAITERS is no longer in the state, we try to contend again.
        if (!(mutex.state.load(std::memory_order_acquire) & FIBER_WAITERS))
            silk::FiberScheduler::releaseWaiters(mutex.getWaiterKey());
    }

    uint64_t getWaiterKey() const { return reinterpret_cast<uint64_t>(&storage.__state); }

    std::__libcpp_mutex_t & storage;
    std::atomic_ref<uint32_t> state;
};

constinit const StdMutexHooks * std_mutex_hooks = nullptr;

constinit thread_local char current_thread_tag = 0;

class RecursiveMutex
{
public:
    explicit RecursiveMutex(std::__libcpp_recursive_mutex_t & storage_)
        : storage(storage_)
    {
    }

    void lock()
    {
        const Identity self = Identity::current();
        if (getOwnerIdentity() != self)
        {
            std::__libcpp_mutex_lock(&storage.__mutex);
            setOwnerIdentity(self);
        }
        ++storage.__count;
    }

    bool tryLock()
    {
        const Identity self = Identity::current();
        if (getOwnerIdentity() != self)
        {
            if (!std::__libcpp_mutex_trylock(&storage.__mutex))
                return false;
            setOwnerIdentity(self);
        }
        ++storage.__count;
        return true;
    }

    void unlock()
    {
        if (--storage.__count == 0)
        {
            setOwnerIdentity({});
            std::__libcpp_mutex_unlock(&storage.__mutex);
        }
    }

private:
    struct Identity
    {
        uint64_t fiber = 0;
        uint64_t thread = 0;

        static Identity current()
        {
            const uint64_t fiber_id = silk::FiberScheduler::getCurrentFiberId().raw;
            return {fiber_id, fiber_id ? 0 : reinterpret_cast<uint64_t>(&current_thread_tag)};
        }

        bool operator==(const Identity &) const = default;
    };

    Identity getOwnerIdentity() const
    {
        return {std::atomic_ref(storage.__owner_fiber).load(std::memory_order_relaxed), std::atomic_ref(storage.__owner_thread).load(std::memory_order_relaxed)};
    }

    void setOwnerIdentity(Identity owner)
    {
        std::atomic_ref(storage.__owner_fiber).store(owner.fiber, std::memory_order_relaxed);
        std::atomic_ref(storage.__owner_thread).store(owner.thread, std::memory_order_relaxed);
    }

    std::__libcpp_recursive_mutex_t & storage;
};

using Waiter = std::__clickhouse_condvar_waiter;

class WaiterList
{
public:
    explicit WaiterList(std::__libcpp_condvar_t & storage_)
        : storage(storage_)
    {
    }

    void push(Waiter & waiter)
    {
        SpinLock lock(storage);
        waiter.prev = storage.__tail;
        if (storage.__tail)
            storage.__tail->next = &waiter;
        else
            setHead(&waiter);
        storage.__tail = &waiter;
        waiter.queued = true;
    }

    bool remove(Waiter & waiter)
    {
        SpinLock lock(storage);
        if (!waiter.queued)
            return false;
        unlink(waiter);
        return true;
    }

    Waiter * popFront()
    {
        if (empty())
            return nullptr;
        SpinLock lock(storage);
        Waiter * const waiter = storage.__head;
        if (waiter)
            unlink(*waiter);
        return waiter;
    }

    Waiter * popAll()
    {
        if (empty())
            return nullptr;
        SpinLock lock(storage);
        Waiter * const waiter = storage.__head;
        for (Waiter * queued = waiter; queued; queued = queued->next)
            queued->queued = false;
        setHead(nullptr);
        storage.__tail = nullptr;
        return waiter;
    }

private:
    class SpinLock
    {
    public:
        explicit SpinLock(std::__libcpp_condvar_t & storage_)
            : storage(storage_)
        {
            std::atomic_ref lock(storage.__lock);
            while (lock.exchange(1, std::memory_order_acquire))
                while (lock.load(std::memory_order_relaxed))
                    silk::cpuPause();
        }

        ~SpinLock() { std::atomic_ref(storage.__lock).store(0, std::memory_order_release); }

    private:
        std::__libcpp_condvar_t & storage;
    };

    bool empty() const { return !std::atomic_ref(storage.__head).load(std::memory_order_acquire); }

    void setHead(Waiter * waiter) { std::atomic_ref(storage.__head).store(waiter, std::memory_order_release); }

    void unlink(Waiter & waiter)
    {
        if (waiter.prev)
            waiter.prev->next = waiter.next;
        else
            setHead(waiter.next);
        if (waiter.next)
            waiter.next->prev = waiter.prev;
        else
            storage.__tail = waiter.prev;
        waiter.queued = false;
    }

    std::__libcpp_condvar_t & storage;
};

class ConditionVariable
{
public:
    explicit ConditionVariable(std::__libcpp_condvar_t & storage_)
        : storage(storage_)
    {
    }

    int wait(Mutex mutex, uint64_t deadline)
    {
        Waiter waiter;
        waiter.fiber = needSilkAwareWait();
        WaiterList(storage).push(waiter);
        mutex.unlock();
        int result = waitForNotify(waiter, deadline);
        if (result == ETIMEDOUT)
        {
            if (tryClaim(waiter, CLAIMED_BY_TIMEOUT))
            {
                if (!WaiterList(storage).remove(waiter))
                    result = 0;
                std::atomic_ref(waiter.claim).store(TIMEOUT_CLEANUP_DONE, std::memory_order_release);
            }
            else
            {
                /// The notifier thread concurrently removed us from the wait list,
                /// wait until it calls notify(*waiter) object before we destroy it.
                waitForNotify(waiter, UINT64_MAX);
                result = 0;
            }
        }
        mutex.lock();
        return result;
    }

    void notifyOne()
    {
        if (Waiter * waiter = WaiterList(storage).popFront())
            notify(*waiter);
    }

    void notifyAll()
    {
        Waiter * waiter = WaiterList(storage).popAll();
        while (waiter)
        {
            Waiter * const next = waiter->next;
            notify(*waiter);
            waiter = next;
        }
    }

private:
    static constexpr uint32_t UNCLAIMED = 0;
    static constexpr uint32_t CLAIMED_BY_NOTIFIER = 1;
    static constexpr uint32_t CLAIMED_BY_TIMEOUT = 2;
    static constexpr uint32_t TIMEOUT_CLEANUP_DONE = 3;

    static bool tryClaim(Waiter & waiter, uint32_t claimant)
    {
        uint32_t expected = UNCLAIMED;
        return std::atomic_ref(waiter.claim).compare_exchange_strong(expected, claimant, std::memory_order_acq_rel);
    }

    static int waitForNotify(Waiter & waiter, uint64_t deadline)
    {
        if (waiter.fiber)
        {
            const uint64_t now = silk::getTimeNanoseconds();
            const uint64_t timeout = deadline > now ? deadline - now : 0;
            /// Silk sleep counts the timeout in 64-bit TSC cycles, which overflow for timeouts of about a century, so we treat anything near that as no timeout.
            constexpr uint64_t max_fiber_timeout = 1'000'000'000ULL * 1'000'000'000;
            if (deadline == UINT64_MAX || timeout >= max_fiber_timeout)
                return waiter.future.wait();
            return silk::FiberFuture::waitWithTimeout(&waiter.future, timeout);
        }

        while (!std::atomic_ref(waiter.notified).load(std::memory_order_acquire))
            if (futexWait(waiter.notified, 0, deadline) == ETIMEDOUT)
                return ETIMEDOUT;
        return 0;
    }

    static void notify(Waiter & waiter)
    {
        if (!tryClaim(waiter, CLAIMED_BY_NOTIFIER))
        {
            while (std::atomic_ref(waiter.claim).load(std::memory_order_acquire) != TIMEOUT_CLEANUP_DONE)
                silk::cpuPause();
            return;
        }
        if (waiter.fiber)
        {
            waiter.future.set(0);
            return;
        }
        uint32_t * const notified = &waiter.notified;
        std::atomic_ref(*notified).store(1, std::memory_order_release);
        futexWake(notified, 1);
    }

    std::__libcpp_condvar_t & storage;
};
}

void setStdMutexHooks(const StdMutexHooks * hooks)
{
    std::atomic_ref(std_mutex_hooks).store(hooks, std::memory_order_relaxed);
}

_LIBCPP_BEGIN_NAMESPACE_STD

int __libcpp_recursive_mutex_init(__libcpp_recursive_mutex_t * m)
{
    *m = {};
    return 0;
}

int __libcpp_recursive_mutex_lock(__libcpp_recursive_mutex_t * m)
{
    RecursiveMutex(*m).lock();
    return 0;
}

bool __libcpp_recursive_mutex_trylock(__libcpp_recursive_mutex_t * m)
{
    return RecursiveMutex(*m).tryLock();
}

int __libcpp_recursive_mutex_unlock(__libcpp_recursive_mutex_t * m)
{
    RecursiveMutex(*m).unlock();
    return 0;
}

int __libcpp_recursive_mutex_destroy(__libcpp_recursive_mutex_t *)
{
    return 0;
}

int __libcpp_mutex_lock(__libcpp_mutex_t * m)
{
    const StdMutexHooks * hooks = std::atomic_ref(std_mutex_hooks).load(std::memory_order_relaxed);
    if (hooks)
        hooks->before_lock();
    Mutex(*m).lock();
    if (hooks)
        hooks->after_lock();
    return 0;
}

bool __libcpp_mutex_trylock(__libcpp_mutex_t * m)
{
    return Mutex(*m).tryLock();
}

int __libcpp_mutex_unlock(__libcpp_mutex_t * m)
{
    const StdMutexHooks * hooks = std::atomic_ref(std_mutex_hooks).load(std::memory_order_relaxed);
    if (hooks)
        hooks->before_unlock();
    Mutex(*m).unlock();
    if (hooks)
        hooks->after_unlock();
    return 0;
}

int __libcpp_mutex_destroy(__libcpp_mutex_t *)
{
    return 0;
}

int __libcpp_condvar_signal(__libcpp_condvar_t * cv)
{
    ConditionVariable(*cv).notifyOne();
    return 0;
}

int __libcpp_condvar_broadcast(__libcpp_condvar_t * cv)
{
    ConditionVariable(*cv).notifyAll();
    return 0;
}

int __libcpp_condvar_wait(__libcpp_condvar_t * cv, __libcpp_mutex_t * m)
{
    ConditionVariable(*cv).wait(Mutex(*m), UINT64_MAX);
    return 0;
}

int __libcpp_condvar_timedwait(__libcpp_condvar_t * cv, __libcpp_mutex_t * m, __libcpp_timespec_t * ts)
{
    timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    const int64_t seconds = static_cast<int64_t>(ts->tv_sec) - now.tv_sec;
    if (seconds < 0)
        return ETIMEDOUT;
    const int64_t nanoseconds = seconds * 1'000'000'000 + (ts->tv_nsec - now.tv_nsec);
    if (nanoseconds <= 0)
        return ETIMEDOUT;
    return ConditionVariable(*cv).wait(Mutex(*m), silk::getTimeNanoseconds() + static_cast<uint64_t>(nanoseconds));
}

int __libcpp_condvar_destroy(__libcpp_condvar_t *)
{
    return 0;
}

_LIBCPP_END_NAMESPACE_STD
