#include <Common/CancelToken.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int THREAD_WAS_CANCELED;
}
}

#ifdef OS_LINUX /// Because of futex

#include <base/getThreadId.h>

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
}

void CancelToken::Registry::insert(CancelToken * token)
{
    std::lock_guard lock(mutex);
    threads[token->thread_id] = token;
}

void CancelToken::Registry::remove(CancelToken * token)
{
    std::lock_guard lock(mutex);
    threads.erase(token->thread_id);
}

void CancelToken::Registry::signal(UInt64 tid)
{
    std::lock_guard lock(mutex);
    if (auto it = threads.find(tid); it != threads.end())
        it->second->signalImpl();
}

void CancelToken::Registry::signal(UInt64 tid, int code, const String & message)
{
    std::lock_guard lock(mutex);
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
            // Start non-cancelable wait on futex. Spurious wake-up is possible.
            futexWait(address, value);
            return true; // Disabled - true is forced
        }
        if (s & canceled)
            return false; // Has already been canceled
        if (state.compare_exchange_strong(s, reinterpret_cast<UInt64>(address)))
            break; // This futex has been "acquired" by this token
    }

    // Start cancelable wait. Spurious wake-up is possible.
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

            s = state.load();
            continue; // To avoid race (may lead to futex destruction) we have to wait for signaling thread to finish
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
    std::unique_lock lock(signal_mutex);
    if (exception_code != 0)
        throw DB::Exception::createRuntime(
            std::exchange(exception_code, 0),
            std::exchange(exception_message, {}));
    throw DB::Exception(ErrorCodes::THREAD_WAS_CANCELED, "Thread was canceled");
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
    std::unique_lock lock(signal_mutex);

    UInt64 s = state.load();
    while (true)
    {
        if (s & canceled)
            return; // Already canceled - don't signal twice
        if (state.compare_exchange_strong(s, s | canceled))
            break; // It is the canceling thread - should deliver signal if necessary
    }

    exception_code = code;
    exception_message = message;

    if ((s & disabled) == disabled)
        return; // cancellation is disabled - just signal token for later, but don't wake
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

    // Wake all threads waiting on `address`, one of them will be canceled and others will get spurious wake-ups
    // Woken canceled thread will reset signaled bit
    futexWake(address, INT_MAX);

    // Signaling thread must remove address from state to notify canceled thread that `futexWake()` is done, thus `wake()` can return.
    // Otherwise we may have race condition: signaling thread may try to wake futex that has been already destructed.
    state.store(canceled);
}

Cancelable::Cancelable()
{
    CancelToken::local().reset();
}

Cancelable::~Cancelable()
{
    CancelToken::local().disable();
}

NonCancelable::NonCancelable()
{
    CancelToken::local().disable();
}

NonCancelable::~NonCancelable()
{
    CancelToken::local().enable();
}

}

#else

namespace DB
{

void CancelToken::raise()
{
    throw DB::Exception(ErrorCodes::THREAD_WAS_CANCELED, "Thread was canceled");
}

}

#endif
