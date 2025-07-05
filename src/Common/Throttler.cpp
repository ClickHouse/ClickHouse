#include <atomic>
#include <Common/Throttler.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentThread.h>
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

Throttler::Throttler(size_t max_speed_, const ThrottlerPtr & parent_,
        ProfileEvents::Event event_amount_,
        ProfileEvents::Event event_sleep_us_)
    : max_speed(max_speed_)
    , max_burst(max_speed_ * default_burst_seconds)
    , limit_exceeded_exception_message("")
    , tokens(max_burst)
    , parent(parent_)
    , event_amount(event_amount_)
    , event_sleep_us(event_sleep_us_)
{}

Throttler::Throttler(size_t max_speed_,
        ProfileEvents::Event event_amount_,
        ProfileEvents::Event event_sleep_us_)
    : Throttler(max_speed_, nullptr, event_amount_, event_sleep_us_)
{}

Throttler::Throttler(size_t max_speed_, size_t limit_, const char * limit_exceeded_exception_message_, const ThrottlerPtr & parent_)
    : max_speed(max_speed_)
    , max_burst(max_speed_ * default_burst_seconds)
    , limit(limit_)
    , limit_exceeded_exception_message(limit_exceeded_exception_message_)
    , tokens(max_burst)
    , parent(parent_)
{}

bool Throttler::throttle(size_t amount, size_t max_block_us)
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

    /// Wait unless there is positive amount of tokens - throttling
    bool block = max_speed_value && tokens_value < 0;
    if (block)
    {
        block_count.fetch_add(1, std::memory_order_relaxed);
        SCOPE_EXIT({block_count.fetch_sub(1, std::memory_order_relaxed);});

        auto & profile_events = CurrentThread::getProfileEvents();
        auto timer = profile_events.timer(ProfileEvents::ThrottlerSleepMicroseconds);
        std::optional<ProfileEvents::Timer> timer2;
        if (event_sleep_us != ProfileEvents::end())
            timer2.emplace(profile_events.timer(event_sleep_us));

        // Note that throwing exception from the following blocking call is safe. It is important for query cancellation.
        Int64 block_ns = static_cast<Int64>(-tokens_value / max_speed_value * NS);
        sleepForNanoseconds(std::min<Int64>(max_block_us * 1000, block_ns));
    }

    bool parent_block = false;
    if (parent)
        parent_block = parent->throttle(amount, max_block_us);

    return block || parent_block;
}

void Throttler::throttleNonBlocking(size_t amount)
{
    size_t count_value = 0;
    double tokens_value = 0.0;
    size_t max_speed_value = 0;
    throttleImpl(amount, count_value, tokens_value, max_speed_value);

    if (event_amount != ProfileEvents::end())
        ProfileEvents::increment(event_amount, amount);

    if (parent)
        parent->throttleNonBlocking(amount);
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

    max_speed = max_speed_;
    max_burst = max_speed_ * default_burst_seconds;
}

}
