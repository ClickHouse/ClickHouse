#pragma once

#include <mutex>
#include <memory>
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <common/sleep.h>
#include <IO/WriteHelpers.h>


namespace ProfileEvents
{
    extern const Event ThrottlerSleepMicroseconds;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}


/** Allows you to limit the speed of something (in entities per second) using sleep.
  * Specifics of work:
  * - only the average speed is considered, from the moment of the first call of `add` function;
  *   if there were periods with low speed, then during some time after them, the speed will be higher;
  *
  * Also allows you to set a limit on the maximum number of entities. If exceeded, an exception will be thrown.
  */
class Throttler
{
public:
    Throttler(size_t max_speed_, const std::shared_ptr<Throttler> & parent_ = nullptr)
            : max_speed(max_speed_), limit_exceeded_exception_message(""), parent(parent_) {}

    Throttler(size_t max_speed_, size_t limit_, const char * limit_exceeded_exception_message_,
              const std::shared_ptr<Throttler> & parent_ = nullptr)
        : max_speed(max_speed_), limit(limit_), limit_exceeded_exception_message(limit_exceeded_exception_message_), parent(parent_) {}

    void add(const size_t amount)
    {
        size_t new_count;
        UInt64 elapsed_ns = 0;

        {
            std::lock_guard lock(mutex);

            if (max_speed)
            {
                if (0 == count)
                {
                    watch.start();
                    elapsed_ns = 0;
                }
                else
                    elapsed_ns = watch.elapsed();
            }

            count += amount;
            new_count = count;
        }

        if (limit && new_count > limit)
            throw Exception(limit_exceeded_exception_message + std::string(" Maximum: ") + toString(limit), ErrorCodes::LIMIT_EXCEEDED);

        if (max_speed)
        {
            /// How much time to wait for the average speed to become `max_speed`.
            UInt64 desired_ns = new_count * 1000000000 / max_speed;

            if (desired_ns > elapsed_ns)
            {
                UInt64 sleep_ns = desired_ns - elapsed_ns;
                sleepForNanoseconds(sleep_ns);

                ProfileEvents::increment(ProfileEvents::ThrottlerSleepMicroseconds, sleep_ns / 1000UL);
            }
        }

        if (parent)
            parent->add(amount);
    }

    /// Not thread safe
    void setParent(const std::shared_ptr<Throttler> & parent_)
    {
        parent = parent_;
    }

    void reset()
    {
        std::lock_guard lock(mutex);

        count = 0;
        watch.reset();
    }

private:
    size_t count = 0;
    const size_t max_speed = 0;
    const UInt64 limit = 0;        /// 0 - not limited.
    const char * limit_exceeded_exception_message = nullptr;
    Stopwatch watch {CLOCK_MONOTONIC_COARSE};
    std::mutex mutex;

    /// Used to implement a hierarchy of throttlers
    std::shared_ptr<Throttler> parent;
};


using ThrottlerPtr = std::shared_ptr<Throttler>;

}
