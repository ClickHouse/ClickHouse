#pragma once

#include <base/types.h>
#include <base/defines.h>

#include <Common/Exception.h>

#ifdef OS_LINUX /// Because of futex

#include <atomic>
#include <mutex>
#include <unordered_map>
#include <memory>

namespace DB
{

// Scoped object, enabling thread cancellation (cannot be nested).
// Intended to be used once per cancelable task. It erases any previously held cancellation signal.
// Note that by default thread is not cancelable.
struct Cancelable
{
    Cancelable();
    ~Cancelable();
};

// Scoped object, disabling thread cancellation (cannot be nested; must be inside `Cancelable` region)
struct NonCancelable
{
    NonCancelable();
    ~NonCancelable();
};

// Responsible for synchronization needed to deliver thread cancellation signal.
// Basic building block for cancelable synchronization primitives.
// Allows to perform cancelable wait on memory addresses (think futex)
class CancelToken
{
public:
    CancelToken();
    CancelToken(const CancelToken &) = delete;
    CancelToken(CancelToken &&) = delete;
    CancelToken & operator=(const CancelToken &) = delete;
    CancelToken & operator=(CancelToken &&) = delete;
    ~CancelToken();

    // Returns token for the current thread
    static CancelToken & local()
    {
        static thread_local CancelToken token;
        return token;
    }

    // Cancelable wait on memory address (futex word).
    //   Thread will do atomic compare-and-sleep `*address == value`. Waiting will continue until `notify_one()`
    //   or `notify_all()` will be called with the same `address` or calling thread will be canceled using `signal()`.
    //   Note that spurious wake-ups are also possible due to cancellation of other waiters on the same `address`.
    //   WARNING: `address` must be 2-byte aligned and `value` highest bit must be zero.
    // Return value:
    //   true - woken by either notify or spurious wakeup;
    //   false - iff cancellation signal has been received.
    // Implementation details:
    //   It registers `address` inside token's `state` to allow other threads to wake this thread and deliver cancellation signal.
    //   Highest bit of `*address` is used for guaranteed delivery of the signal, but is guaranteed to be zero on return due to cancellation.
    // Intended to be called only by thread associated with this token.
    bool wait(UInt32 * address, UInt32 value);

    // Throws `DB::Exception` received from `signal()`. Call it if `wait()` returned false.
    // Intended to be called only by thread associated with this token.
    [[noreturn]] void raise();

    // Regular wake by address (futex word). It does not interact with token in any way. We have it here to complement `wait()`.
    // Can be called from any thread.
    static void notifyOne(UInt32 * address);
    static void notifyAll(UInt32 * address);

    // Send cancel signal to thread with specified `tid`.
    // If thread was waiting using `wait()` it will be woken up (unless cancellation is disabled).
    // Can be called from any thread.
    static void signal(UInt64 tid);
    static void signal(UInt64 tid, int code, const String & message);

    // Flag used to deliver cancellation into memory address to wake a thread.
    // Note that most significant bit at `addresses` to be used with `wait()` is reserved.
    static constexpr UInt32 signaled = 1u << 31u;

private:
    friend struct Cancelable;
    friend struct NonCancelable;

    // Restores initial state for token to be reused. See `Cancelable` struct.
    // Intended to be called only by thread associated with this token.
    void reset()
    {
        state.store(0);
    }

    // Enable thread cancellation. See `NonCancelable` struct.
    // Intended to be called only by thread associated with this token.
    void enable()
    {
        chassert((state.load() & disabled) == disabled);
        state.fetch_and(~disabled);
    }

    // Disable thread cancellation. See `NonCancelable` struct.
    // Intended to be called only by thread associated with this token.
    void disable()
    {
        chassert((state.load() & disabled) == 0);
        state.fetch_or(disabled);
    }

    // Singleton. Maps thread IDs to tokens.
    struct Registry
    {
        std::mutex mutex;
        std::unordered_map<UInt64, CancelToken*> threads; // By thread ID

        void insert(CancelToken * token);
        void remove(CancelToken * token);
        void signal(UInt64 tid);
        void signal(UInt64 tid, int code, const String & message);

        static const std::shared_ptr<Registry> & instance();
    };

    // Cancels this token and wakes thread if necessary.
    // Can be called from any thread.
    void signalImpl();
    void signalImpl(int code, const String & message);

    // Lower bit: cancel signal received flag
    static constexpr UInt64 canceled = 1;

    // Upper bits - possible values:
    // 1) all zeros: token is enabed, i.e. wait() call can return false, thread is not waiting on any address;
    // 2) all ones: token is disabled, i.e. wait() call cannot be canceled;
    // 3) specific `address`: token is enabled and thread is currently waiting on this `address`.
    static constexpr UInt64 disabled = ~canceled;
    static_assert(sizeof(UInt32 *) == sizeof(UInt64)); // State must be able to hold an address

    // All signal handling logic should be globally serialized using this mutex
    static std::mutex signal_mutex;

    // Cancellation state
    alignas(64) std::atomic<UInt64> state;
    [[maybe_unused]] char padding[64 - sizeof(state)];

    // Cancellation exception
    int exception_code;
    String exception_message;

    // Token is permanently attached to a single thread. There is one-to-one mapping between threads and tokens.
    const UInt64 thread_id;

    // To avoid `Registry` destruction before last `Token` destruction
    const std::shared_ptr<Registry> registry;
};

}

#else

// WARNING: We support cancelable synchronization primitives only on linux for now

namespace DB
{

struct Cancelable
{
    Cancelable() = default;
    ~Cancelable() = default;
};

struct NonCancelable
{
    NonCancelable() = default;
    ~NonCancelable() = default;
};

class CancelToken
{
public:
    CancelToken() = default;
    CancelToken(const CancelToken &) = delete;
    CancelToken(CancelToken &&) = delete;
    CancelToken & operator=(const CancelToken &) = delete;
    ~CancelToken() = default;

    static CancelToken & local()
    {
        static CancelToken token;
        return token;
    }

    bool wait(UInt32 *, UInt32) { return true; }
    [[noreturn]] void raise();
    static void notifyOne(UInt32 *) {}
    static void notifyAll(UInt32 *) {}
    static void signal(UInt64) {}
    static void signal(UInt64, int, const String &) {}
};

}

#endif
