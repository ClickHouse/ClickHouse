#include <Common/Threading.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int THREAD_WAS_CANCELLED;
}
}

#ifdef OS_LINUX /// Because of futex

#include <base/getThreadId.h>

#include <bit>

#include <linux/futex.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace DB
{

namespace
{
    inline Int64 futexWait(void * address, UInt32 value)
    {
        return syscall(SYS_futex, address, FUTEX_WAIT_PRIVATE, value, nullptr, nullptr, 0);
    }

    inline Int64 futexWake(void * address, int count)
    {
        return syscall(SYS_futex, address, FUTEX_WAKE_PRIVATE, count, nullptr, nullptr, 0);
    }

    // inline void waitFetch(std::atomic<UInt32> & address, UInt32 & value)
    // {
    //     futexWait(&address, value);
    //     value = address.load();
    // }

    // inline void wakeOne(std::atomic<UInt32> & address)
    // {
    //     futexWake(&address, 1);
    // }

    // inline void wakeAll(std::atomic<UInt32> & address)
    // {
    //      futexWake(&address, INT_MAX);
    // }

    inline constexpr UInt32 lowerValue(UInt64 value)
    {
        return static_cast<UInt32>(value & 0xffffffffull);
    }

    inline constexpr UInt32 upperValue(UInt64 value)
    {
        return static_cast<UInt32>(value >> 32ull);
    }

    inline UInt32 * lowerAddress(void * address)
    {
        return reinterpret_cast<UInt32 *>(address) + (std::endian::native == std::endian::big);
    }

    inline UInt32 * upperAddress(void * address)
    {
        return reinterpret_cast<UInt32 *>(address) + (std::endian::native == std::endian::little);
    }

    inline void waitLowerFetch(std::atomic<UInt64> & address, UInt64 & value)
    {
        futexWait(lowerAddress(&address), lowerValue(value));
        value = address.load();
    }

    inline bool cancellableWaitLowerFetch(std::atomic<UInt64> & address, UInt64 & value)
    {
        bool res = CancelToken::local().wait(lowerAddress(&address), lowerValue(value));
        value = address.load();
        return res;
    }

    inline void wakeLowerOne(std::atomic<UInt64> & address)
    {
        syscall(SYS_futex, lowerAddress(&address), FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0);
    }

    // inline void wakeLowerAll(std::atomic<UInt64> & address)
    // {
    //     syscall(SYS_futex, lowerAddress(&address), FUTEX_WAKE_PRIVATE, INT_MAX, nullptr, nullptr, 0);
    // }

    inline void waitUpperFetch(std::atomic<UInt64> & address, UInt64 & value)
    {
        futexWait(upperAddress(&address), upperValue(value));
        value = address.load();
    }

    inline bool cancellableWaitUpperFetch(std::atomic<UInt64> & address, UInt64 & value)
    {
        bool res = CancelToken::local().wait(upperAddress(&address), upperValue(value));
        value = address.load();
        return res;
    }

    // inline void wakeUpperOne(std::atomic<UInt64> & address)
    // {
    //     syscall(SYS_futex, upperAddress(&address), FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0);
    // }

    inline void wakeUpperAll(std::atomic<UInt64> & address)
    {
        syscall(SYS_futex, upperAddress(&address), FUTEX_WAKE_PRIVATE, INT_MAX, nullptr, nullptr, 0);
    }
}

void CancelToken::Registry::insert(CancelToken * token)
{
    std::lock_guard<std::mutex> lock(mutex);
    threads[token->thread_id] = token;
}

void CancelToken::Registry::remove(CancelToken * token)
{
    std::lock_guard<std::mutex> lock(mutex);
    threads.erase(token->thread_id);
}

void CancelToken::Registry::signal(UInt64 tid)
{
    std::lock_guard<std::mutex> lock(mutex);
    if (auto it = threads.find(tid); it != threads.end())
        it->second->signalImpl();
}

void CancelToken::Registry::signal(UInt64 tid, int code, const String & message)
{
    std::lock_guard<std::mutex> lock(mutex);
    if (auto it = threads.find(tid); it != threads.end())
        it->second->signalImpl(code, message);
}

const std::shared_ptr<CancelToken::Registry> & CancelToken::Registry::instance()
{
    static std::shared_ptr<Registry> registry{new Registry()}; // shared_ptr is used to enforce correct destruction order of tokens and registry
    return registry;
}

CancelToken::CancelToken()
    : state(disabled)
    , thread_id(getThreadId())
    , registry(Registry::instance())
{
    registry->insert(this);
}

CancelToken::~CancelToken()
{
    registry->remove(this);
}

void CancelToken::signal(UInt64 tid)
{
    Registry::instance()->signal(tid);
}

void CancelToken::signal(UInt64 tid, int code, const String & message)
{
    Registry::instance()->signal(tid, code, message);
}

bool CancelToken::wait(UInt32 * address, UInt32 value)
{
    chassert((reinterpret_cast<UInt64>(address) & canceled) == 0); // An `address` must be 2-byte aligned
    if (value & signaled) // Can happen after spurious wake-up due to cancel of other thread
        return true; // Spin-wait unless signal is handled

    UInt64 s = state.load();
    while (true)
    {
        if (s & disabled)
        {
            // Start non-cancellable wait on futex. Spurious wake-up is possible.
            futexWait(address, value);
            return true; // Disabled - true is forced
        }
        if (s & canceled)
            return false; // Has already been canceled
        if (state.compare_exchange_strong(s, reinterpret_cast<UInt64>(address)))
            break; // This futex has been "acquired" by this token
    }

    // Start cancellable wait. Spurious wake-up is possible.
    futexWait(address, value);

    // "Release" futex and check for cancellation
    s = state.load();
    while (true)
    {
        chassert((s & disabled) != disabled); // `disable()` must not be called from another thread
        if (s & canceled)
        {
            if (s == canceled)
                break; // Signaled; futex "release" has been done by the signaling thread
            else
            {
                s = state.load();
                continue; // To avoid race (may lead to futex destruction) we have to wait for signaling thread to finish
            }
        }
        if (state.compare_exchange_strong(s, 0))
            return true; // There was no cancellation; futex "released"
    }

    // Reset signaled bit
    reinterpret_cast<std::atomic<UInt32> *>(address)->fetch_and(~signaled);
    return false;
}

void CancelToken::raise()
{
    std::unique_lock<std::mutex> lock(signal_mutex);
    if (exception_code != 0)
        throw DB::Exception(
            std::exchange(exception_code, 0),
            std::exchange(exception_message, {}));
    else
        throw DB::Exception(ErrorCodes::THREAD_WAS_CANCELLED, "Thread was cancelled");
}

void CancelToken::notifyOne(UInt32 * address)
{
    futexWake(address, 1);
}

void CancelToken::notifyAll(UInt32 * address)
{
    futexWake(address, INT_MAX);
}

void CancelToken::signalImpl()
{
    signalImpl(0, {});
}

std::mutex CancelToken::signal_mutex;

void CancelToken::signalImpl(int code, const String & message)
{
    // Serialize all signaling threads to avoid races due to concurrent signal()/raise() calls
    std::unique_lock<std::mutex> lock(signal_mutex);

    UInt64 s = state.load();
    while (true)
    {
        if (s & canceled)
            return; // Already cancelled - don't signal twice
        if (state.compare_exchange_strong(s, s | canceled))
            break; // It is the cancelling thread - should deliver signal if necessary
    }

    exception_code = code;
    exception_message = message;

    if ((s & disabled) == disabled)
        return; // Cancellation is disabled - just signal token for later, but don't wake
    std::atomic<UInt32> * address = reinterpret_cast<std::atomic<UInt32> *>(s & disabled);
    if (address == nullptr)
        return; // Thread is currently not waiting on futex - wake-up not required

    // Set signaled bit
    UInt32 value = address->load();
    while (true)
    {
        if (value & signaled) // Already signaled, just spin-wait until previous signal is handled by waiter
            value = address->load();
        else if (address->compare_exchange_strong(value, value | signaled))
            break;
    }

    // Wake all threads waiting on `address`, one of them will be cancelled and others will get spurious wake-ups
    // Woken canceled thread will reset signaled bit
    futexWake(address, INT_MAX);

    // Signaling thread must remove address from state to notify canceled thread that `futexWake()` is done, thus `wake()` can return.
    // Otherwise we may have race condition: signaling thread may try to wake futex that has been already destructed.
    state.store(canceled);
}

Cancellable::Cancellable()
{
    CancelToken::local().reset();
}

Cancellable::~Cancellable()
{
    CancelToken::local().disable();
}

NonCancellable::NonCancellable()
{
    CancelToken::local().disable();
}

NonCancellable::~NonCancellable()
{
    CancelToken::local().enable();
}

CancellableSharedMutex::CancellableSharedMutex()
    : state(0)
    , waiters(0)
{}

void CancellableSharedMutex::lock()
{
    UInt64 value = state.load();
    while (true)
    {
        if (value & writers)
        {
            waiters++;
            if (!cancellableWaitUpperFetch(state, value))
            {
                waiters--;
                CancelToken::local().raise();
            }
            else
                waiters--;
        }
        else if (state.compare_exchange_strong(value, value | writers))
            break;
    }

    value |= writers;
    while (value & readers)
    {
        if (!cancellableWaitLowerFetch(state, value))
        {
            state.fetch_and(~writers);
            wakeUpperAll(state);
            CancelToken::local().raise();
        }
    }
}

bool CancellableSharedMutex::try_lock()
{
    UInt64 value = state.load();
    return (value & (readers | writers)) == 0 && state.compare_exchange_strong(value, value | writers);
}

void CancellableSharedMutex::unlock()
{
    state.fetch_and(~writers);
    if (waiters)
        wakeUpperAll(state);
}

void CancellableSharedMutex::lock_shared()
{
    UInt64 value = state.load();
    while (true)
    {
        if (value & writers)
        {
            waiters++;
            if (!cancellableWaitUpperFetch(state, value))
            {
                waiters--;
                CancelToken::local().raise();
            }
            else
                waiters--;
        }
        else if (state.compare_exchange_strong(value, value + 1)) // overflow is not realistic
            break;
    }
}

bool CancellableSharedMutex::try_lock_shared()
{
    UInt64 value = state.load();
    if (!(value & writers) && state.compare_exchange_strong(value, value + 1)) // overflow is not realistic
        return true;
    return false;
}

void CancellableSharedMutex::unlock_shared()
{
    UInt64 value = state.fetch_sub(1) - 1;
    if ((value & (writers | readers)) == writers) // If writer is waiting and no more readers
        wakeLowerOne(state); // Wake writer
}

FastSharedMutex::FastSharedMutex()
    : state(0)
    , waiters(0)
{}

void FastSharedMutex::lock()
{
    UInt64 value = state.load();
    while (true)
    {
        if (value & writers)
        {
            waiters++;
            waitUpperFetch(state, value);
            waiters--;
        }
        else if (state.compare_exchange_strong(value, value | writers))
            break;
    }

    value |= writers;
    while (value & readers)
        waitLowerFetch(state, value);
}

bool FastSharedMutex::try_lock()
{
    UInt64 value = 0;
    if (state.compare_exchange_strong(value, writers))
        return true;
    return false;
}

void FastSharedMutex::unlock()
{
    state.store(0);
    if (waiters)
        wakeUpperAll(state);
}

void FastSharedMutex::lock_shared()
{
    UInt64 value = state.load();
    while (true)
    {
        if (value & writers)
        {
            waiters++;
            waitUpperFetch(state, value);
            waiters--;
        }
        else if (state.compare_exchange_strong(value, value + 1))
            break;
    }
}

bool FastSharedMutex::try_lock_shared()
{
    UInt64 value = state.load();
    if (!(value & writers) && state.compare_exchange_strong(value, value + 1))
        return true;
    return false;
}

void FastSharedMutex::unlock_shared()
{
    UInt64 value = state.fetch_sub(1) - 1;
    if (value == writers)
        wakeLowerOne(state); // Wake writer
}

}

#else

namespace DB
{

void CancelToken::raise()
{
    throw DB::Exception(ErrorCodes::THREAD_WAS_CANCELLED, "Thread was cancelled");
}

}

#endif
