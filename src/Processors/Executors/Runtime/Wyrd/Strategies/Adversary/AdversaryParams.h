#pragma once

#include <chrono>
#include <cstddef>

namespace DB::Runtime::Wyrd
{

struct AdversaryParams
{
    size_t yield_one_in = 10;
    size_t spurious_failure_one_in = 11;
    size_t suspend_one_in = 10;
    size_t max_suspended_threads = 2;
    size_t resume_one_in = 5;
    bool pin_to_single_core = true;
    std::chrono::seconds watchdog_timeout{60};
};

}
