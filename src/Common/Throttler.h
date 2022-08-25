#pragma once

#include <Common/Throttler_fwd.h>

#include <mutex>
#include <memory>
#include <base/sleep.h>
#include <atomic>

namespace DB
{

/** Allows you to limit the speed of something (in entities per second) using sleep.
  * Specifics of work:
  *  Tracks exponentially (pow of 1/2) smoothed speed with hardcoded window.
  *  See more comments in .cpp file.
  *
  * Also allows you to set a limit on the maximum number of entities. If exceeded, an exception will be thrown.
  */
class Throttler
{
public:
    explicit Throttler(size_t max_speed_, const std::shared_ptr<Throttler> & parent_ = nullptr)
            : max_speed(max_speed_), limit_exceeded_exception_message(""), parent(parent_) {}

    Throttler(size_t max_speed_, size_t limit_, const char * limit_exceeded_exception_message_,
              const std::shared_ptr<Throttler> & parent_ = nullptr)
        : max_speed(max_speed_), limit(limit_), limit_exceeded_exception_message(limit_exceeded_exception_message_), parent(parent_) {}

    /// Calculates the smoothed speed, sleeps if required and throws exception on
    /// limit overflow.
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
    const size_t max_speed{0};
    const uint64_t limit{0};        /// 0 - not limited.
    const char * limit_exceeded_exception_message = nullptr;
    std::mutex mutex;
    std::atomic<uint64_t> accumulated_sleep{0};
    /// Smoothed value of current speed. Updated in `add` method.
    double smoothed_speed{0};
    /// previous `add` call time (in nanoseconds)
    uint64_t prev_ns{0};

    /// Used to implement a hierarchy of throttlers
    std::shared_ptr<Throttler> parent;
};

}
