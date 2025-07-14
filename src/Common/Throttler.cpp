#include <Common/Throttler.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
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

/// Just 10^9.
static constexpr auto NS = 1000000000UL;

Throttler::Throttler(size_t max_speed_, const std::shared_ptr<Throttler> & parent_)
    : max_speed(max_speed_)
    , max_burst(max_speed_ * default_burst_seconds)
    , limit_exceeded_exception_message("")
    , tokens(max_burst)
    , parent(parent_)
{}

Throttler::Throttler(size_t max_speed_, size_t limit_, const char * limit_exceeded_exception_message_,
            const std::shared_ptr<Throttler> & parent_)
    : max_speed(max_speed_)
    , max_burst(max_speed_ * default_burst_seconds)
    , limit(limit_)
    , limit_exceeded_exception_message(limit_exceeded_exception_message_)
    , tokens(max_burst)
    , parent(parent_)
{}

UInt64 Throttler::add(size_t amount)
{
    // Values obtained under lock to be checked after release
    size_t count_value = 0;
    double tokens_value = 0.0;
    addImpl(amount, count_value, tokens_value);

    if (limit && count_value > limit)
        throw Exception::createDeprecated(limit_exceeded_exception_message + std::string(" Maximum: ") + toString(limit), ErrorCodes::LIMIT_EXCEEDED);

    /// Wait unless there is positive amount of tokens - throttling
    Int64 sleep_time_ns = 0;
    if (max_speed && tokens_value < 0)
    {
        sleep_time_ns = static_cast<Int64>(-tokens_value / max_speed * NS);
        accumulated_sleep += sleep_time_ns;
        sleepForNanoseconds(sleep_time_ns);
        accumulated_sleep -= sleep_time_ns;
        ProfileEvents::increment(ProfileEvents::ThrottlerSleepMicroseconds, sleep_time_ns / 1000UL);
    }

    if (parent)
        sleep_time_ns += parent->add(amount);

    return static_cast<UInt64>(sleep_time_ns);
}

void Throttler::addImpl(size_t amount, size_t & count_value, double & tokens_value)
{
    std::lock_guard lock(mutex);
    auto now = clock_gettime_ns_adjusted(prev_ns);
    if (max_speed)
    {
        double delta_seconds = prev_ns ? static_cast<double>(now - prev_ns) / NS : 0;
        tokens = std::min<double>(tokens + max_speed * delta_seconds - amount, max_burst);
    }
    count += amount;
    count_value = count;
    tokens_value = tokens;
    prev_ns = now;
}

void Throttler::reset()
{
    std::lock_guard lock(mutex);

    count = 0;
    tokens = max_burst;
    prev_ns = 0;
    // NOTE: do not zero `accumulated_sleep` to avoid races
}

bool Throttler::isThrottling() const
{
    if (accumulated_sleep != 0)
        return true;

    if (parent)
        return parent->isThrottling();

    return false;
}

Int64 Throttler::getAvailable()
{
    // To update bucket state and receive current number of token in a thread-safe way
    size_t count_value = 0;
    double tokens_value = 0.0;
    addImpl(0, count_value, tokens_value);

    return static_cast<Int64>(tokens_value);
}

}
