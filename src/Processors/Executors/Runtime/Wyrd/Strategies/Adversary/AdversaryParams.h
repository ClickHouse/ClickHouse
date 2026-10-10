#pragma once

#include <chrono>
#include <cstddef>

namespace DB::Runtime::Wyrd
{

struct AdversaryParams
{
    /// Main adversary params.
    bool pin_to_single_core = true;
    std::chrono::seconds watchdog_timeout{60};

    /// Fault injection: how often a thread is disturbed at a fault point.
    double yield_probability = 0.1;
    double spurious_failure_probability = 0.1;
    double suspend_probability = 0.1;

    /// Lock-freedom check: suspending threads inside a lock-free scope and resuming them on the progress of others.
    size_t max_suspended_threads = 2;
    double resume_probability = 0.2;
};

}
