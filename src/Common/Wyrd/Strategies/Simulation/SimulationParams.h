#pragma once

#include <Common/CoroutineStack.h>
#include <base/types.h>

namespace DB::Runtime::Wyrd
{

struct SimulationParams
{
    /// Main simulation params.
    UInt64 seed = 42;
    size_t max_steps = 100'000;
    size_t fiber_stack_size = CoroutineStack::default_stack_size;
    bool collect_timeline = false;

    /// Fault injection: how often a thread is disturbed at a sync point.
    double preempt_probability = 0.1;
    double spurious_wakeup_probability = 0.15;
    double spurious_failure_probability = 0.15;
    double lock_free_suspend_probability = 0.15;

    /// Lock-freedom check: holding threads inside a lock-free scope and resuming them on the progress of others.
    size_t max_suspended_threads = 1;
    double resume_probability = 0.25;
};

}
