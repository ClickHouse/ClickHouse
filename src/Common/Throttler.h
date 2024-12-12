#pragma once

#include <Common/Throttler_fwd.h>
#include <Common/ProfileEvents.h>

#include <mutex>
#include <memory>
#include <base/sleep.h>
#include <base/types.h>
#include <atomic>

namespace DB
{

/** Allows you to limit the speed of something (in tokens per second) using sleep.
  * Implemented using Token Bucket Throttling algorithm.
  * Also allows you to set a limit on the maximum number of tokens. If exceeded, an exception will be thrown.
  */
class Throttler
{
public:
    static const size_t default_burst_seconds = 1;

    Throttler(size_t max_speed_, size_t max_burst_, const std::shared_ptr<Throttler> & parent_ = nullptr)
        : max_speed(max_speed_), max_burst(max_burst_), limit_exceeded_exception_message(""), tokens(max_burst), parent(parent_) {}

    explicit Throttler(size_t max_speed_, const std::shared_ptr<Throttler> & parent_ = nullptr);

    Throttler(size_t max_speed_, size_t max_burst_, size_t limit_, const char * limit_exceeded_exception_message_,
              const std::shared_ptr<Throttler> & parent_ = nullptr)
        : max_speed(max_speed_), max_burst(max_burst_), limit(limit_), limit_exceeded_exception_message(limit_exceeded_exception_message_), tokens(max_burst), parent(parent_) {}

    Throttler(size_t max_speed_, size_t limit_, const char * limit_exceeded_exception_message_,
              const std::shared_ptr<Throttler> & parent_ = nullptr);

    /// Use `amount` tokens, sleeps if required or throws exception on limit overflow.
    /// Returns duration of sleep in nanoseconds (to distinguish sleeping on different kinds of throttlers for metrics)
    UInt64 add(size_t amount);

    UInt64 add(size_t amount, ProfileEvents::Event event_amount, ProfileEvents::Event event_sleep_us)
    {
        UInt64 sleep_ns = add(amount);
        ProfileEvents::increment(event_amount, amount);
        ProfileEvents::increment(event_sleep_us, sleep_ns / 1000UL);
        return sleep_ns;
    }

    /// Not thread safe
    void setParent(const std::shared_ptr<Throttler> & parent_)
    {
        parent = parent_;
    }

    /// Reset all throttlers internal stats
    void reset();

    /// Is throttler already accumulated some sleep time and throttling.
    bool isThrottling() const;

    Int64 getAvailable();
    UInt64 getMaxSpeed() const { return static_cast<UInt64>(max_speed); }
    UInt64 getMaxBurst() const { return static_cast<UInt64>(max_burst); }

private:
    void addImpl(size_t amount, size_t & count_value, double & tokens_value);

    size_t count{0};
    const size_t max_speed{0}; /// in tokens per second.
    const size_t max_burst{0}; /// in tokens.
    const UInt64 limit{0}; /// 0 - not limited.
    const char * limit_exceeded_exception_message = nullptr;
    std::mutex mutex;
    std::atomic<UInt64> accumulated_sleep{0}; // Accumulated sleep time over all waiting threads
    double tokens{0}; /// Amount of tokens available in token bucket. Updated in `add` method.
    UInt64 prev_ns{0}; /// Previous `add` call time (in nanoseconds).

    /// Used to implement a hierarchy of throttlers
    std::shared_ptr<Throttler> parent;
};

}
