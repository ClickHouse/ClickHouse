#pragma once

#include <Common/CoroutineStack.h>
#include <base/types.h>

namespace DB::Runtime::Wyrd
{

struct SimulationParams
{
    UInt64 seed = 42;
    size_t max_syncs_before_preempt = 15;
    size_t spurious_wakeup_one_in = 7;
    size_t spurious_failure_one_in = 7;
    size_t lock_free_suspend_one_in = 7;
    size_t max_suspended_threads = 1;
    size_t resume_after_progress_hints = 4;
    size_t max_steps = 100'000;
    size_t fiber_stack_size = CoroutineStack::default_stack_size;
    bool collect_timeline = false;
};

}
