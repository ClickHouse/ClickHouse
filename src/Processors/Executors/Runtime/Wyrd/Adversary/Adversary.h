#pragma once

#include <base/defines.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

namespace DB::Runtime::Wyrd
{

struct AdversaryParams
{
    size_t yield_freq = 10;
    size_t spurious_failure_every = 11;
    size_t suspend_freq = 10;
    size_t suspended_threads_limit = 2;
    size_t resume_freq = 5;
    bool pin_to_single_core = true;
};

struct AdversaryResult
{
    size_t yields;
    size_t suspends;
    size_t spurious_failures;
};

/// Yields at random fault points, fails compare_exchange_weak spuriously, and inside a lock-free scope suspends a bounded number of threads until the others make progress.
class Adversary
{
    struct SuspendedThread;

    void suspendThisThread();
    void resumeRandomThread();
    void maybeYield();

public:
    explicit Adversary(AdversaryParams params_);

    void reset();
    AdversaryResult report() const;

    void threadEnter();
    void threadFault();
    bool threadSpuriousFailure();
    void threadSetLockFree(bool on);
    void threadProgress();
    void threadExit();

private:
    const AdversaryParams params;

    std::atomic<size_t> thread_count{0};
    std::atomic<size_t> suspended_count{0};
    std::atomic<size_t> suspend_total{0};
    std::atomic<size_t> yield_count{0};
    std::atomic<size_t> spurious_failure_count{0};

    std::mutex mutex;
    std::vector<std::shared_ptr<SuspendedThread>> suspended TSA_GUARDED_BY(mutex);
};

}
