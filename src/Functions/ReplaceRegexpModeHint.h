#pragma once

#include <base/types.h>

#include <atomic>


namespace DB
{

/// What `ReplaceRegexpImpl` learned about whether caching results per distinct haystack pays off, carried
/// from one call of a function to the next, i.e. from one block to the next. A block is often shorter than
/// the probes need to settle, so without it every block would pay for the first probes again.
/// The threads that execute the same function share it, hence the atomics. Relaxed ordering is enough:
/// a stale or mixed state only selects a slower way to compute the same result.
struct ReplaceRegexpModeHint
{
    std::atomic<bool> known{false};
    std::atomic<bool> cache_is_cheaper{false};
    std::atomic<UInt32> probe_interval{0};
    std::atomic<UInt32> windows_until_probe{0};
};

}
