#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRetry.h>

#include <Common/thread_local_rng.h>

#include <algorithm>
#include <limits>

namespace DB::Cas
{

uint64_t Retry::backoff(uint32_t attempt)
{
    if (attempt == 0)
        return 0;
    const uint32_t doublings = std::min<uint32_t>(attempt - 1, 20);
    const uint64_t ceiling = std::min<uint64_t>(5000, 200ull << doublings);
    return thread_local_rng() % (ceiling + 1);     /// full jitter: uniform(0, ceiling)
}

Retry::Bound Retry::bind(uint64_t now_ms) const
{
    const uint64_t own_deadline_ms = policy_deadline_ms
        ? *policy_deadline_ms
        : (now_ms > std::numeric_limits<uint64_t>::max() - window_ms
               ? std::numeric_limits<uint64_t>::max()
               : now_ms + window_ms);
    if (lease_deadline_ms && *lease_deadline_ms < own_deadline_ms)
        return {*lease_deadline_ms, true};
    return {own_deadline_ms, false};
}

}
