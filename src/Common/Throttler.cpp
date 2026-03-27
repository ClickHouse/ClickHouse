#include <Common/Throttler.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>

#include <base/scope_guard.h>

#include <atomic>
#include <limits>

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

Throttler::Throttler(const char * throttler_name_, size_t max_speed_, const ThrottlerPtr & parent_,
        ProfileEvents::Event event_amount_,
        ProfileEvents::Event event_sleep_us_)
    : throttler_name(throttler_name_)
    , max_speed(max_speed_)
    , max_burst(max_speed_ * default_burst_seconds)
    , limit_exceeded_exception_message("")
    , tokens(static_cast<double>(max_burst))
    , parent(parent_)
    , event_amount(event_amount_)
    , event_sleep_us(event_sleep_us_)
{}

Throttler::Throttler(const char * throttler_name_, size_t max_speed_,
        ProfileEvents::Event event_amount_,
        ProfileEvents::Event event_sleep_us_)
    : Throttler(throttler_name_, max_speed_, nullptr, event_amount_, event_sleep_us_)
{}

Throttler::Throttler(const char * throttler_name_, size_t max_speed_, size_t limit_, const char * limit_exceeded_exception_message_, const ThrottlerPtr & parent_)
    : throttler_name(throttler_name_)
    , max_speed(max_speed_)
    , max_burst(max_speed_ * default_burst_seconds)
    , limit(limit_)
    , limit_exceeded_exception_message(limit_exceeded_exception_message_)
    , tokens(static_cast<double>(max_burst))
    , parent(parent_)
{}

bool Throttler::throttle(size_t amount, size_t max_block_ns)
{
    // Values obtained under lock to be checked after release
    size_t count_value = 0;
    double tokens_value = 0.0;
    size_t max_speed_value = 0;
    throttleImpl(amount, count_value, tokens_value, max_speed_value);
    if (event_amount != ProfileEvents::end())
        ProfileEvents::increment(event_amount, amount);

    if (limit && count_value > limit)
        throw Exception::createDeprecated(limit_exceeded_exception_message + std::string(" Maximum: ") + toString(limit), ErrorCodes::LIMIT_EXCEEDED);

    // Wait unless there is positive amount of tokens - throttling
    bool block = max_block_ns > 0 && max_speed_value > 0 && tokens_value < 0;
    if (block)
    {
        block_count.fetch_add(1, std::memory_order_relaxed);
        SCOPE_EXIT({
            block_count.fetch_sub(1, std::memory_order_relaxed);
        });

        auto & profile_events = CurrentThread::getProfileEvents();
        auto timer = profile_events.timer(ProfileEvents::ThrottlerSleepMicroseconds);
        std::optional<ProfileEvents::Timer> timer2;
        if (event_sleep_us != ProfileEvents::end())
            timer2.emplace(profile_events.timer(event_sleep_us));

        // Calculate how long to sleep
        double block_ns_double = -tokens_value / static_cast<double>(max_speed_value) * NS;
        chassert(block_ns_double >= 0.0);

        // Clamp to be safe and avoid any UB
        UInt64 block_ns = block_ns_double >= static_cast<double>(std::numeric_limits<UInt64>::max())
            ? std::numeric_limits<UInt64>::max()
            : static_cast<UInt64>(block_ns_double);

        UInt64 sleep_ns = std::min<UInt64>(max_block_ns, block_ns);
        LOG_TRACE(log, "Sleeping: throttler_name={}, amount={}, count={}, tokens={}, max_speed={}, block_ns={}, max_block_ns={}, sleep_ns={}", throttler_name, amount, count_value, tokens_value, max_speed_value, block_ns, max_block_ns, sleep_ns);

        // Note that throwing exception from the following blocking call is safe. It is important for query cancellation.
        sleepForNanoseconds(sleep_ns);
    }
    else if (max_speed_value > 0)
    {
        LOG_TRACE(log, "Not sleeping: throttler_name={}, amount={}, count={}, tokens={}, max_speed={}", throttler_name, amount, count_value, tokens_value, max_speed_value);
    }

    bool parent_block = false;
    if (parent)
        parent_block = parent->throttle(amount, max_block_ns);

    return block || parent_block;
}

void Throttler::throttleImpl(size_t amount, size_t & count_value, double & tokens_value)
{
    size_t max_speed_value = 0;
    throttleImpl(amount, count_value, tokens_value, max_speed_value);
}

void Throttler::throttleImpl(size_t amount, size_t & count_value, double & tokens_value, size_t & max_speed_value)
{
    std::lock_guard lock(mutex);
    auto now = clock_gettime_ns_adjusted(prev_ns);
    if (max_speed)
    {
        max_speed_value = max_speed;
        double delta_seconds = prev_ns ? static_cast<double>(now - prev_ns) / NS : 0;
        tokens = std::min<double>(
            tokens + static_cast<double>(max_speed) * delta_seconds - static_cast<double>(amount), static_cast<double>(max_burst));
    }
    count += amount;
    count_value = count;
    tokens_value = tokens;
    prev_ns = now;
}

void Throttler::reset()
{
    std::lock_guard lock(mutex);

    LOG_TRACE(log, "Reset: throttler_name={}, count={}, tokens={}, max_burst={}", throttler_name, count, tokens, max_burst);

    count = 0;
    tokens = static_cast<double>(max_burst);
    prev_ns = 0;
    // NOTE: do not zero `accumulated_sleep` to avoid races
}

bool Throttler::isThrottling() const
{
    return block_count.load(std::memory_order_relaxed) > 0 || (parent && parent->isThrottling());
}

Int64 Throttler::getAvailable()
{
    // To update bucket state and receive current number of token in a thread-safe way
    size_t count_value = 0;
    double tokens_value = 0.0;
    throttleImpl(0, count_value, tokens_value);

    return static_cast<Int64>(tokens_value);
}

UInt64 Throttler::getMaxSpeed() const
{
    std::lock_guard lock(mutex);
    return max_speed;
}

UInt64 Throttler::getMaxBurst() const
{
    std::lock_guard lock(mutex);
    return max_burst;
}

void Throttler::setMaxSpeed(size_t max_speed_)
{
    std::lock_guard lock(mutex);

    LOG_TRACE(log, "setMaxSpeed: throttler_name={}, {} -> {}, tokens={}", throttler_name, max_speed, max_speed_, tokens);

    max_speed = max_speed_;
    max_burst = max_speed_ * default_burst_seconds;
}

}
