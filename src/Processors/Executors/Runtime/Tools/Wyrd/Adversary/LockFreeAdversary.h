#pragma once

#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/IAdversary.h>

#include <cstddef>

namespace DB::Runtime::Wyrd
{

struct LockFreeAdversaryParams
{
    size_t suspend_freq = 10;
    size_t suspended_threads_limit = 2;
    size_t resume_freq = 5;
    size_t yield_freq = 10;
};

/// Like the yield adversary, but inside a lock-free scope it also suspends a bounded number of threads until the others make progress.
AdversaryPtr createLockFreeAdversary(LockFreeAdversaryParams params);

}
