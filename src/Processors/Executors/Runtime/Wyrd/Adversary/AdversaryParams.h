#pragma once

#include <cstddef>

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

}
