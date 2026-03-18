#pragma once
#include <cstdint>
#include <atomic>
#include <base/defines.h>

namespace DB
{

/** Allows to randomize thread scheduling and insert various glitches across whole program for testing purposes.
  * It is done by setting up a timer that will send PROF signal to every thread when certain amount of CPU time has passed.
  *
  * To initialize ThreadFuzzer, call ThreadFuzzer::instance().
  * The behaviour is controlled by environment variables:
  *
  * THREAD_FUZZER_CPU_TIME_PERIOD_US  - period of signals in microseconds.
  * THREAD_FUZZER_YIELD_PROBABILITY   - probability to do 'sched_yield'.
  * THREAD_FUZZER_MIGRATE_PROBABILITY - probability to set CPU affinity to random CPU core.
  * THREAD_FUZZER_SLEEP_PROBABILITY   - probability to sleep.
  * THREAD_FUZZER_SLEEP_TIME_US_MAX   - max amount of time to sleep in microseconds, actual sleep time is randomized.
  *
  * ThreadFuzzer will do nothing if environment variables are not set accordingly.
  *
  * The intention is to reproduce thread synchronization bugs (race conditions and deadlocks) more frequently in tests.
  * We already have tests with TSan. But TSan only covers "physical" synchronization bugs, but not "logical" ones,
  *  where all data is protected by synchronization primitives, but we still have race conditions.
  * Obviously, TSan cannot debug distributed synchronization bugs.
  *
  * The motivation for this tool is an evidence, that concurrency bugs are more likely to reproduce
  *  on bad unstable virtual machines in a dirty environments.
  *
  * The idea is not new, see also:
  * https://channel9.msdn.com/blogs/peli/concurrency-fuzzing-with-cuzz
  *
  * Notes:
  * - it can be also implemented with instrumentation (example: LLVM Xray) instead of signals.
  *
  * In addition, we allow to inject glitches around thread synchronization functions.
  * Example:
  *
  * THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_PROBABILITY=0.001
  * THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_TIME_US_MAX=10000
  * THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_PROBABILITY=0.001
  * THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_TIME_US_MAX=10000
  */
class ThreadFuzzer
{
public:
    static ThreadFuzzer & instance()
    {
        static ThreadFuzzer res;
        return res;
    }

    bool isEffective() const;
    bool needsSetup() const;

    static void stop();
    static void start();
    static bool ALWAYS_INLINE isStarted();

    static void maybeInjectSleep();
    static void maybeInjectMemoryLimitException();

private:
    uint64_t cpu_time_period_us = 0;
    double yield_probability = 0;
    double migrate_probability = 0;
    double sleep_probability = 0;
    double sleep_time_us_max = 0;

    double explicit_sleep_probability = 0;
    double explicit_memory_exception_probability = 0;

    inline static std::atomic<bool> started{true};

    ThreadFuzzer();

    void initConfiguration();
    void setup() const;

    static void signalHandler(int);
};

}
