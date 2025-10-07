#pragma once

#include <Common/IThrottler.h>
#include <Common/ProfileEvents.h>

#include <mutex>
#include <base/sleep.h>
#include <base/types.h>
#include <atomic>

namespace DB
{

/** Allows you to limit the speed of something (in tokens per second) using sleep.
  * Implemented using Token Bucket Throttling algorithm.
  * Also allows you to set a limit on the maximum number of tokens. If exceeded, an exception will be thrown.
  */
class Throttler : public IThrottler
{
public:
    static const size_t default_burst_seconds = 1;

    Throttler(size_t max_speed_, size_t max_burst_, const ThrottlerPtr & parent_ = nullptr,
            ProfileEvents::Event event_amount_ = ProfileEvents::end(),
            ProfileEvents::Event event_sleep_us_ = ProfileEvents::end())
        : max_speed(max_speed_), max_burst(max_burst_), limit_exceeded_exception_message(""), tokens(max_burst), parent(parent_)
        , event_amount(event_amount_), event_sleep_us(event_sleep_us_)
    {}

    Throttler(size_t max_speed_, size_t max_burst_,
            ProfileEvents::Event event_amount_ = ProfileEvents::end(),
            ProfileEvents::Event event_sleep_us_ = ProfileEvents::end())
        : max_speed(max_speed_), max_burst(max_burst_), limit_exceeded_exception_message(""), tokens(max_burst)
        , event_amount(event_amount_), event_sleep_us(event_sleep_us_)
    {}

    explicit Throttler(size_t max_speed_, const ThrottlerPtr & parent_ = nullptr,
        ProfileEvents::Event event_amount_ = ProfileEvents::end(),
        ProfileEvents::Event event_sleep_us_ = ProfileEvents::end());

    Throttler(size_t max_speed_,
        ProfileEvents::Event event_amount_,
        ProfileEvents::Event event_sleep_us_);

    Throttler(size_t max_speed_, size_t max_burst_, size_t limit_, const char * limit_exceeded_exception_message_,
              const ThrottlerPtr & parent_ = nullptr)
        : max_speed(max_speed_), max_burst(max_burst_), limit(limit_), limit_exceeded_exception_message(limit_exceeded_exception_message_), tokens(max_burst), parent(parent_) {}

    Throttler(size_t max_speed_, size_t limit_, const char * limit_exceeded_exception_message_,
              const ThrottlerPtr & parent_ = nullptr);

    /// Use `amount` tokens, sleeps if required or throws exception on limit overflow.
    /// Returns duration of sleep in nanoseconds (to distinguish sleeping on different kinds of throttlers for metrics)
    UInt64 add(size_t amount) override;

    /// Not thread safe
    void setParent(const ThrottlerPtr & parent_)
    {
        parent = parent_;
    }

    /// Reset all throttlers internal stats
    void reset();

    /// Is throttler already accumulated some sleep time and throttling.
    bool isThrottling() const override;

    Int64 getAvailable() override;
    UInt64 getMaxSpeed() const override;
    UInt64 getMaxBurst() const override;

    void setMaxSpeed(size_t max_speed_);

private:
    void addImpl(size_t amount, size_t & count_value, double & tokens_value);
    void addImpl(size_t amount, size_t & count_value, double & tokens_value, size_t & max_speed_value);

    size_t count{0};
    size_t max_speed TSA_GUARDED_BY(mutex){0}; /// in tokens per second.
    size_t max_burst TSA_GUARDED_BY(mutex){0}; /// in tokens.
    const UInt64 limit{0}; /// 0 - not limited.
    const char * limit_exceeded_exception_message = nullptr;
    mutable std::mutex mutex;
    std::atomic<UInt64> accumulated_sleep{0}; // Accumulated sleep time over all waiting threads
    double tokens{0}; /// Amount of tokens available in token bucket. Updated in `add` method.
    UInt64 prev_ns{0}; /// Previous `add` call time (in nanoseconds).

    /// Used to implement a hierarchy of throttlers
    ThrottlerPtr parent;

    /// Event to increment when throttler uses tokens
    ProfileEvents::Event event_amount{ProfileEvents::end()};

    /// Event to increment when throttler sleeps
    ProfileEvents::Event event_sleep_us{ProfileEvents::end()};
};

}
