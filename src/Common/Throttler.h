#pragma once

#include <Common/Throttler_fwd.h>

#include <mutex>
#include <memory>
#include <base/sleep.h>
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
    Throttler(size_t max_speed_, size_t max_burst_, const std::shared_ptr<Throttler> & parent_ = nullptr)
        : max_speed(max_speed_), max_burst(max_burst_), limit_exceeded_exception_message(""), tokens(max_burst), parent(parent_) {}

    explicit Throttler(size_t max_speed_, const std::shared_ptr<Throttler> & parent_ = nullptr);

    Throttler(size_t max_speed_, size_t max_burst_, size_t limit_, const char * limit_exceeded_exception_message_,
              const std::shared_ptr<Throttler> & parent_ = nullptr)
        : max_speed(max_speed_), max_burst(max_burst_), limit(limit_), limit_exceeded_exception_message(limit_exceeded_exception_message_), tokens(max_burst), parent(parent_) {}

    Throttler(size_t max_speed_, size_t limit_, const char * limit_exceeded_exception_message_,
              const std::shared_ptr<Throttler> & parent_ = nullptr);

    /// Use `amount` tokens, sleeps if required or throws exception on limit overflow.
    void add(size_t amount);

    /// Not thread safe
    void setParent(const std::shared_ptr<Throttler> & parent_)
    {
        parent = parent_;
    }

    /// Reset all throttlers internal stats
    void reset();

    /// Is throttler already accumulated some sleep time and throttling.
    bool isThrottling() const;

private:
    size_t count{0};
    const size_t max_speed{0}; /// in tokens per second.
    const size_t max_burst{0}; /// in tokens.
    const uint64_t limit{0}; /// 0 - not limited.
    const char * limit_exceeded_exception_message = nullptr;
    std::mutex mutex;
    std::atomic<uint64_t> accumulated_sleep{0}; // Accumulated sleep time over all waiting threads
    double tokens{0}; /// Amount of tokens available in token bucket. Updated in `add` method.
    uint64_t prev_ns{0}; /// Previous `add` call time (in nanoseconds).

    /// Used to implement a hierarchy of throttlers
    std::shared_ptr<Throttler> parent;
};

}
