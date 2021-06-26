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

uint64_t ACLMap::convertACLs(const Coordination::ACLs & acls)
{
    if (acl_to_num.count(acls))
        return acl_to_num[acls];

    /// Start from one
    auto index = acl_to_num.size() + 1;

    acl_to_num[acls] = index;
    num_to_acl[index] = acls;

    return index;
}

Coordination::ACLs ACLMap::convertNumber(uint64_t acls_id) const
{
    if (acls_id == 0)
        return Coordination::ACLs{};

    if (!num_to_acl.count(acls_id))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown ACL id {}. It's a bug", acls_id);

    return num_to_acl.at(acls_id);
}

void ACLMap::addMapping(uint64_t acls_id, const Coordination::ACLs & acls)
{
    num_to_acl[acls_id] = acls;
    acl_to_num[acls] = acls_id;
}

void ACLMap::addUsage(uint64_t acl_id)
{
    usage_counter[acl_id]++;
}

void ACLMap::removeUsage(uint64_t acl_id)
{
    if (usage_counter.count(acl_id) == 0)
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
