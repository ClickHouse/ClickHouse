#pragma once

#include <Common/CoroutineStack.h>
#include <base/types.h>

namespace DB::Runtime::Wyrd
{

struct SimulationParams
{
    UInt64 seed = 42;
    size_t time_slice = 15;
    size_t spurious_wakeup_every = 7;
    size_t spurious_failure_every = 7;
    size_t hold_every = 7;
    size_t held_threads_limit = 1;
    size_t progress_to_resume = 4;
    size_t step_limit = 100'000;
    size_t stack_size = CoroutineStack::default_stack_size;
};

}
