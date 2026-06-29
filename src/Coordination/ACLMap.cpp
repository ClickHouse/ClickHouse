#include <Coordination/ACLMap.h>
#include <Common/SipHash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

size_t ACLMap::ACLsHash::operator()(const Coordination::ACLs & acls) const
{
    SipHash hash;
    for (const auto & acl : acls)
    {
        hash.update(acl.permissions);
        hash.update(acl.scheme);
        hash.update(acl.id);
    }
    return hash.get64();
}

bool ACLMap::ACLsComparator::operator()(const Coordination::ACLs & left, const Coordination::ACLs & right) const
{
    if (left.size() != right.size())
        return false;

    for (size_t i = 0; i < left.size(); ++i)
    {
        if (left[i].permissions != right[i].permissions)
            return false;

        if (left[i].scheme != right[i].scheme)
            return false;

        if (left[i].id != right[i].id)
            return false;
    }
    return true;
}

ACLMap::MapEntry & ACLMap::numToAcl(ACLId id)
{
    auto it = num_to_acl.find(id);
    if (it == num_to_acl.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ACL with id {} does not exist", id);
    return it->second;
}

ACLId ACLMap::convertACLs(const Coordination::ACLs & acls)
{
    if (acls.empty())
        return 0;

    {
        std::shared_lock shared_lock(map_mutex);
        if (auto it = acl_to_num.find(acls); it != acl_to_num.end())
        {
            ++numToAcl(it->second).usage;
            return it->second;
        }
    }

    std::lock_guard lock(map_mutex);

    /// Re-check after re-locking.
    {
        if (auto it = acl_to_num.find(acls); it != acl_to_num.end())
        {
            ++numToAcl(it->second).usage;
            return it->second;
        }
    }

    /// Start from one. After overflow, skip zero (sentinel) and IDs still in use.
    auto start = max_acl_id;
    auto index = max_acl_id++;
    while (index == 0 || num_to_acl.contains(index))
    {
        index = max_acl_id++;
        if (max_acl_id == start)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "All ACL IDs are in use");
    }

    bool emplaced = acl_to_num.emplace(acls, index).second;
    chassert(emplaced);
    emplaced = num_to_acl.emplace(std::piecewise_construct,
        std::forward_as_tuple(index), std::forward_as_tuple(acls, 1)).second;
    chassert(emplaced);

    return index;
}

Coordination::ACLs ACLMap::convertNumber(ACLId acls_id) const
{
    if (acls_id == 0)
        return Coordination::ACLs{};

    std::shared_lock shared_lock(map_mutex);

    auto it = num_to_acl.find(acls_id);
    if (it == num_to_acl.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown ACL id {}. It's a bug", acls_id);

    return it->second.acls;
}

std::vector<std::pair<ACLId, Coordination::ACLs>> ACLMap::getMapping() const
{
    std::shared_lock shared_lock(map_mutex);
    std::vector<std::pair<ACLId, Coordination::ACLs>> vec;
    vec.reserve(num_to_acl.size());
    for (const auto & [id, entry] : num_to_acl)
        vec.emplace_back(id, entry.acls);
    return vec;
}

void ACLMap::addMapping(ACLId acls_id, const Coordination::ACLs & acls)
{
    std::lock_guard lock(map_mutex);

    if (!num_to_acl.emplace(std::piecewise_construct,
            std::forward_as_tuple(acls_id), std::forward_as_tuple(acls, 0)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ACL with id {} already exists", acls_id);
    if (auto [it, inserted] = acl_to_num.emplace(acls, acls_id); !inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ACLs with id {} and {} are identical", acls_id, it->second);
    max_acl_id = std::max(acls_id + 1, max_acl_id); /// max_acl_id pointer next slot
}

void ACLMap::addUsage(ACLId acl_id)
{
    if (acl_id == 0)
        return;

    std::shared_lock shared_lock(map_mutex);
    numToAcl(acl_id).usage++;
}

void ACLMap::removeUsage(ACLId acl_id)
{
    if (acl_id == 0)
        return;

    {
        /// Fast path: decrement without locking the mutex exclusively.
        std::shared_lock shared_lock(map_mutex);
        uint64_t usage = --numToAcl(acl_id).usage;
        chassert(usage < INT64_MAX);
        if (usage != 0)
            return;
    }

    std::lock_guard lock(map_mutex);

    auto it = num_to_acl.find(acl_id);
    if (it == num_to_acl.end() || it->second.usage.load(std::memory_order_relaxed) != 0)
        /// Benign race condition:
        ///  1. We decrement usage to 0 and unlock mutex.
        ///  2. Another thread does convertACLs and increments usage to 1.
        ///
        /// Then:
        ///  3a. We re-lock the mutex and see usage == 1. No action needed.
        ///
        /// Or:
        ///  3b. Another thread does another removeUsage and decrements usage to 0 again, and
        ///      removes the acl from maps.
        ///  4b. We re-lock the mutex and see that acl_id is not even in the map. No action needed.
        return;

    bool erased = acl_to_num.erase(it->second.acls);
    chassert(erased);
    num_to_acl.erase(it);
}

void ACLMap::removeUnusedACLs()
{
    std::lock_guard lock(map_mutex);

    for (auto it = num_to_acl.begin(); it != num_to_acl.end();)
    {
        if (it->second.usage.load(std::memory_order_relaxed) == 0)
        {
            bool erased = acl_to_num.erase(it->second.acls);
            chassert(erased);
            it = num_to_acl.erase(it);
        }
        else
            ++it;
    }
}

}
