#pragma once

#include <common/time.h>
#include <common/types.h>

#include <atomic>


inline UInt64 clock_gettime_ns(clockid_t clock_type = CLOCK_MONOTONIC)
{
    struct timespec ts;
    clock_gettime(clock_type, &ts);
    return UInt64(ts.tv_sec * 1000000000LL + ts.tv_nsec);
}

/** Differs from Poco::Stopwatch only by using 'clock_gettime' instead of 'gettimeofday',
  *  returns nanoseconds instead of microseconds, and also by other minor differencies.
  */
class Stopwatch
{
public:
    /** CLOCK_MONOTONIC works relatively efficient (~15 million calls/sec) and doesn't lead to syscall.
      * Pass CLOCK_MONOTONIC_COARSE, if you need better performance with acceptable cost of several milliseconds of inaccuracy.
      */
    Stopwatch(clockid_t clock_type_ = CLOCK_MONOTONIC) : clock_type(clock_type_) { start(); }

    void start()                       { start_ns = nanoseconds(); is_running = true; }
    void stop()                        { stop_ns = nanoseconds(); is_running = false; }
    void reset()                       { start_ns = 0; stop_ns = 0; is_running = false; }
    void restart()                     { start(); }
    UInt64 elapsed() const             { return elapsedNanoseconds(); }
    UInt64 elapsedNanoseconds() const  { return is_running ? nanoseconds() - start_ns : stop_ns - start_ns; }
    UInt64 elapsedMicroseconds() const { return elapsedNanoseconds() / 1000U; }
    UInt64 elapsedMilliseconds() const { return elapsedNanoseconds() / 1000000UL; }
    double elapsedSeconds() const      { return static_cast<double>(elapsedNanoseconds()) / 1000000000ULL; }

private:
    UInt64 start_ns = 0;
    UInt64 stop_ns = 0;
    clockid_t clock_type;
    bool is_running = false;

    UInt64 nanoseconds() const { return clock_gettime_ns(clock_type); }
};


class AtomicStopwatch
{
public:
    AtomicStopwatch(clockid_t clock_type_ = CLOCK_MONOTONIC) : clock_type(clock_type_) { restart(); }

    void restart()                     { start_ns = nanoseconds(); }
    UInt64 elapsed() const             { return nanoseconds() - start_ns; }
    UInt64 elapsedMilliseconds() const { return elapsed() / 1000000UL; }
    double elapsedSeconds() const      { return static_cast<double>(elapsed()) / 1000000000ULL; }

    /** If specified amount of time has passed, then restarts timer and returns true.
      * Otherwise returns false.
      * This is done atomically.
      */
    bool compareAndRestart(double seconds)
    {
        UInt64 threshold = static_cast<UInt64>(seconds * 1000000000.0);
        UInt64 current_ns = nanoseconds();
        UInt64 current_start_ns = start_ns;

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

        Lock() {}

        operator bool() const { return parent != nullptr; }

        Lock(AtomicStopwatch * parent_) : parent(parent_) {}

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
        UInt64 current_ns = nanoseconds();
        UInt64 current_start_ns = start_ns;

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
    UInt64 nanoseconds() const { return clock_gettime_ns(clock_type) & 0x7FFFFFFFFFFFFFFFULL; }
};


/// Like ordinary StopWatch, but uses getrusage() system call
struct StopwatchRUsage
{
    StopwatchRUsage() = default;

    void start()   { start_ts = Timestamp::current(); is_running = true; }
    void stop()    { stop_ts = Timestamp::current(); is_running = false; }
    void reset()   { start_ts = Timestamp(); stop_ts = Timestamp(); is_running = false; }
    void restart() { start(); }

    UInt64 elapsed(bool count_user = true, bool count_sys = true) const
    {
        return elapsedNanoseconds(count_user, count_sys);
    }

    UInt64 elapsedNanoseconds(bool count_user = true, bool count_sys = true) const
    {
        return (is_running ? Timestamp::current() : stop_ts).nanoseconds(count_user, count_sys) - start_ts.nanoseconds(count_user, count_sys);
    }

    UInt64 elapsedMicroseconds(bool count_user = true, bool count_sys = true) const
    {
        return elapsedNanoseconds(count_user, count_sys) / 1000UL;
    }

    UInt64 elapsedMilliseconds(bool count_user = true, bool count_sys = true) const
    {
        return elapsedNanoseconds(count_user, count_sys) / 1000000UL;
    }

    double elapsedSeconds(bool count_user = true, bool count_sys = true) const
    {
        return static_cast<double>(elapsedNanoseconds(count_user, count_sys)) / 1000000000.0;
    }

private:

    struct Timestamp
    {
        UInt64 user_ns = 0;
        UInt64 sys_ns = 0;

        static Timestamp current();

        UInt64 nanoseconds(bool count_user = true, bool count_sys = true) const
        {
            return (count_user ? user_ns : 0) + (count_sys ? sys_ns : 0);
        }
    };

    Timestamp start_ts;
    Timestamp stop_ts;
    bool is_running = false;
};


template <typename TStopwatch>
class StopwatchGuard : public TStopwatch
{
public:
    explicit StopwatchGuard(UInt64 & elapsed_ns_) : elapsed_ns(elapsed_ns_) {}

    ~StopwatchGuard() { elapsed_ns += TStopwatch::elapsedNanoseconds(); }

private:
    UInt64 & elapsed_ns;
};
