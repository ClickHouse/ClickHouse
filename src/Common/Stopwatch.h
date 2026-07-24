#pragma once

#include <base/time.h>
#include <base/types.h>
#include <base/defines.h>

#include <cassert>
#include <atomic>
#include <memory>
#include <system_error>
#include <cerrno>


/// From clock_getres(2):
///
///    Similar to CLOCK_MONOTONIC, but provides access to a raw hardware-based
///    time that is not subject to NTP adjustments or the incremental
///    adjustments performed by adjtime(3).
#ifdef CLOCK_MONOTONIC_RAW
static constexpr clockid_t STOPWATCH_DEFAULT_CLOCK = CLOCK_MONOTONIC_RAW;
#else
static constexpr clockid_t STOPWATCH_DEFAULT_CLOCK = CLOCK_MONOTONIC;
#endif

inline UInt64 clock_gettime_ns(clockid_t clock_type = STOPWATCH_DEFAULT_CLOCK)
{
    struct timespec ts;
    if (0 != clock_gettime(clock_type, &ts))
        throw std::system_error(std::error_code(errno, std::system_category()));
    return UInt64(ts.tv_sec * 1000000000LL + ts.tv_nsec);
}

/// Takes previously returned value and returns it again if time stepped back for some reason.
///
/// You should use this if OS does not support CLOCK_MONOTONIC_RAW
inline UInt64 clock_gettime_ns_adjusted(UInt64 prev_time, clockid_t clock_type = STOPWATCH_DEFAULT_CLOCK)
{
#ifdef CLOCK_MONOTONIC_RAW
    if (likely(clock_type == CLOCK_MONOTONIC_RAW))
        return clock_gettime_ns(clock_type);
#endif

    UInt64 current_time = clock_gettime_ns(clock_type);
    if (likely(prev_time <= current_time))
        return current_time;

    /// Something probably went completely wrong if time stepped back for more than 1 second.
    assert(prev_time - current_time <= 1000000000ULL);
    return prev_time;
}

/** Differs from Poco::Stopwatch only by using 'clock_gettime' instead of 'gettimeofday',
  *  returns nanoseconds instead of microseconds, and also by other minor differences.
  */
class Stopwatch
{
public:
    /** CLOCK_MONOTONIC/CLOCK_MONOTONIC_RAW works relatively efficient (~40-50 million calls/sec) and doesn't lead to syscall.
      * Pass CLOCK_MONOTONIC_COARSE, if you need better performance with acceptable cost of several milliseconds of inaccuracy.
      */
    explicit Stopwatch(clockid_t clock_type_ = STOPWATCH_DEFAULT_CLOCK) : clock_type(clock_type_) { start(); }
    explicit Stopwatch(clockid_t clock_type_, UInt64 start_nanoseconds, bool is_running_)
        : start_ns(start_nanoseconds), clock_type(clock_type_), is_running(is_running_)
    {
    }

    void start()                       { start_ns = nanoseconds(); is_running = true; }
    void stop()                        { stop_ns = nanoseconds(); is_running = false; }
    void reset()                       { start_ns = 0; stop_ns = 0; is_running = false; }
    void restart()                     { start(); }
    UInt64 elapsed() const             { return elapsedNanoseconds(); }
    UInt64 elapsedNanoseconds() const  { return is_running ? nanoseconds() - start_ns : stop_ns - start_ns; }
    UInt64 elapsedMicroseconds() const { return elapsedNanoseconds() / 1000U; }
    UInt64 elapsedMilliseconds() const { return elapsedNanoseconds() / 1000000UL; }
    double elapsedSeconds() const      { return static_cast<double>(elapsedNanoseconds()) / 1000000000ULL; }

    UInt64 getStart() const { return start_ns; }
    UInt64 getEnd() const { return stop_ns; }

private:
    UInt64 start_ns = 0;
    UInt64 stop_ns = 0;
    clockid_t clock_type;
    bool is_running = false;

    UInt64 nanoseconds() const { return clock_gettime_ns_adjusted(start_ns, clock_type); }
};

using StopwatchUniquePtr = std::unique_ptr<Stopwatch>;


/// Allows to obtain the elapsed time concurrently with restarting the stopwatch.
/// Allows to atomically compare the elapsed time with a threshold and restart the watch if the elapsed time is not less.
class AtomicStopwatch
{
public:
    explicit AtomicStopwatch(clockid_t clock_type_ = STOPWATCH_DEFAULT_CLOCK) : clock_type(clock_type_) { restart(); }

    void restart()                     { start_ns = nanoseconds(0); }
    UInt64 elapsed() const
    {
        UInt64 current_start_ns = start_ns;
        return nanoseconds(current_start_ns) - current_start_ns;
    }
    UInt64 elapsedMilliseconds() const { return elapsed() / 1000000UL; }
    double elapsedSeconds() const      { return static_cast<double>(elapsed()) / 1000000000ULL; }

    /** If specified amount of time has passed, then restarts timer and returns true.
      * Otherwise returns false.
      * This is done atomically.
      */
    bool compareAndRestart(double seconds)
    {
        UInt64 threshold = static_cast<UInt64>(seconds * 1000000000.0);
        UInt64 current_start_ns = start_ns;
        UInt64 current_ns = nanoseconds(current_start_ns);

        while (true)
        {
            if (current_ns < current_start_ns + threshold)
                return false;

            if (start_ns.compare_exchange_weak(current_start_ns, current_ns))
                return true;
        }
    }

    struct Lock
    {
        AtomicStopwatch * parent = nullptr;

        Lock() = default;

        explicit operator bool() const { return parent != nullptr; }

        explicit Lock(AtomicStopwatch * parent_) : parent(parent_) {}

        Lock(Lock &&) = default;

        ~Lock()
        {
            if (parent)
                parent->restart();
        }
    };

    /** If specified amount of time has passed and timer is not locked right now, then returns Lock object,
      *  which locks timer and, on destruction, restarts timer and releases the lock.
      * Otherwise returns object, that is implicitly casting to false.
      * This is done atomically.
      *
      * Usage:
      * if (auto lock = timer.compareAndRestartDeferred(1))
      *        /// do some work, that must be done in one thread and not more frequently than each second.
      */
    Lock compareAndRestartDeferred(double seconds)
    {
        UInt64 threshold = UInt64(seconds * 1000000000.0);
        UInt64 current_start_ns = start_ns;
        UInt64 current_ns = nanoseconds(current_start_ns);

        while (true)
        {
            if ((current_start_ns & 0x8000000000000000ULL))
                return {};

            if (current_ns < current_start_ns + threshold)
                return {};

            if (start_ns.compare_exchange_weak(current_start_ns, current_ns | 0x8000000000000000ULL))
                return Lock(this);
        }
    }

private:
    std::atomic<UInt64> start_ns;
    std::atomic<bool> lock {false};
    clockid_t clock_type;

    /// Most significant bit is a lock. When it is set, compareAndRestartDeferred method will return false.
    UInt64 nanoseconds(UInt64 prev_time) const { return clock_gettime_ns_adjusted(prev_time, clock_type) & 0x7FFFFFFFFFFFFFFFULL; }
};
