#include <Access/Quota.h>
#include <boost/range/algorithm/equal.hpp>


namespace DB
{
bool operator ==(const Quota::Limits & lhs, const Quota::Limits & rhs)
{
    return boost::range::equal(lhs.max, rhs.max) && (lhs.duration == rhs.duration)
        && (lhs.randomize_interval == rhs.randomize_interval);
}


bool Quota::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_quota = typeid_cast<const Quota &>(other);
    return (all_limits == other_quota.all_limits) && (key_type == other_quota.key_type) && (to_roles == other_quota.to_roles);
}

std::vector<UUID> Quota::findDependencies() const
{
    return to_roles.findDependencies();
}

bool Quota::hasDependencies(const std::unordered_set<UUID> & ids) const
{
    return to_roles.hasDependencies(ids);
}

void Quota::replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids)
{
    to_roles.replaceDependencies(old_to_new_ids);
}

void Quota::copyDependenciesFrom(const IAccessEntity & src, const std::unordered_set<UUID> & ids)
{
    if (getType() != src.getType())
        return;
    const auto & src_quota = typeid_cast<const Quota &>(src);
    to_roles.copyDependenciesFrom(src_quota.to_roles, ids);
}

void Quota::removeDependencies(const std::unordered_set<UUID> & ids)
{
    to_roles.removeDependencies(ids);
}

void Quota::clearAllExceptDependencies()
{
    all_limits.clear();
    key_type = QuotaKeyType::NONE;
}

}
