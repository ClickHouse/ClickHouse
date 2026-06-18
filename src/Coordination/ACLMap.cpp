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

ACLId ACLMap::convertACLs(const Coordination::ACLs & acls)
{
    if (acls.empty())
        return 0;

    std::lock_guard lock(map_mutex);
    if (acl_to_num.contains(acls))
        return acl_to_num[acls];

    /// Start from one. After overflow, skip zero (sentinel) and IDs still in use.
    auto start = max_acl_id;
    auto index = max_acl_id++;
    while (index == 0 || num_to_acl.contains(index))
    {
        index = max_acl_id++;
        if (max_acl_id == start)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "All ACL IDs are in use");
    }

    acl_to_num[acls] = index;
    num_to_acl[index] = acls;

    return index;
}

Coordination::ACLs ACLMap::convertNumber(ACLId acls_id) const
{
    if (acls_id == 0)
        return Coordination::ACLs{};

    std::lock_guard lock(map_mutex);
    if (!num_to_acl.contains(acls_id))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown ACL id {}. It's a bug", acls_id);

    return num_to_acl.at(acls_id);
}

void ACLMap::addMapping(ACLId acls_id, const Coordination::ACLs & acls)
{
    std::lock_guard lock(map_mutex);
    num_to_acl[acls_id] = acls;
    acl_to_num[acls] = acls_id;
    max_acl_id = std::max(acls_id + 1, max_acl_id); /// max_acl_id pointer next slot
}

void ACLMap::addUsage(ACLId acl_id)
{
    std::lock_guard lock(map_mutex);
    usage_counter[acl_id]++;
}

void ACLMap::removeUsage(ACLId acl_id)
{
    std::lock_guard lock(map_mutex);
    if (!usage_counter.contains(acl_id))
        return;

    usage_counter[acl_id]--;

    if (usage_counter[acl_id] == 0)
    {
        auto acls = num_to_acl[acl_id];
        num_to_acl.erase(acl_id);
        acl_to_num.erase(acls);
        usage_counter.erase(acl_id);
    }
}

}
