#pragma once

#include <Common/HashTable/HashSet.h>

#include <cstddef>
#include <memory>
#include <mutex>


namespace DB
{

/// A query-wide bound on distinct native PromQL output label groups.
///
/// Parallel range-sum lanes and their final merge share one instance. The same
/// output group can have an aggregate state in more than one lane without
/// consuming additional group-limit capacity.
class PromQLGroupLimit
{
public:
    explicit PromQLGroupLimit(size_t max_groups_) : max_groups(max_groups_) {}

    bool tryRegister(UInt64 group)
    {
        std::lock_guard lock(mutex);
        if (groups.contains(group))
            return true;
        if (groups.size() >= max_groups)
            return false;
        groups.insert(group);
        return true;
    }

private:
    const size_t max_groups;
    std::mutex mutex;
    HashSet<UInt64, HashCRC32<UInt64>> groups;
};

using PromQLGroupLimitPtr = std::shared_ptr<PromQLGroupLimit>;

}
