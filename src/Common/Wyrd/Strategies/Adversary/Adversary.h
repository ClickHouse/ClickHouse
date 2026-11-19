#pragma once

#include <Processors/Executors/Runtime/Wyrd/Strategies/Adversary/AdversaryParams.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/defines.h>

#include <atomic>
#include <memory>
#include <mutex>

namespace DB::Runtime::Wyrd
{

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
    void threadEnterLockFreeScope();
    void threadLeaveLockFreeScope();
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
    VectorWithMemoryTracking<std::shared_ptr<SuspendedThread>> suspended TSA_GUARDED_BY(mutex);
};

}
