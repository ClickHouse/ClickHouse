#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>

namespace DB::Cas
{

class Pool;

/// Accounting for detached CAS work. Held by `shared_ptr` and owned by the pool AND by every task
/// lease, so it outlives the pool it accounts for: a lease must be able to finish releasing after the
/// pool is already gone.
struct DetachedRegistryState
{
    std::mutex mutex;
    std::condition_variable cv;
    uint64_t in_flight = 0;
    bool stopping = false;
};

/// Read-only view of the registry, handed to every task. The ONLY way a task asks whether teardown has
/// begun; there is deliberately no accessor that also takes a pin, because reading a flag must not
/// create the very in-flight work the reader is trying to avoid.
class DetachedStopToken
{
public:
    explicit DetachedStopToken(std::shared_ptr<DetachedRegistryState> state_)
        : state(std::move(state_))
    {
    }
    bool stopping() const;

private:
    std::shared_ptr<DetachedRegistryState> state;
};

/// Copyable, completes ONCE -- on destruction of the last copy. The task travels through
/// `std::function<void()>` and is copied, so a move-only lease would not compile and a plainly
/// copyable one would release per copy.
///
/// Release order is load-bearing: the pool reference is dropped BEFORE the count is decremented, so a
/// zero count means no tracked task still holds the pool. A task body must therefore NOT capture a
/// pool reference of its own: such a capture dies with the `std::function`, outside this order.
///
/// Construction allocates; arming is separate and happens under the registry mutex, so an allocation
/// failure can never leave a count that nothing will decrement.
class DetachedTaskLease
{
public:
    DetachedTaskLease(std::shared_ptr<Pool> owner, std::shared_ptr<DetachedRegistryState> state,
                      std::function<void()> release_hook);

    /// Arm the completion. Until this is called, destruction releases the owner and touches no count.
    void arm();

private:
    struct Completion;
    std::shared_ptr<Completion> completion;
};

}
