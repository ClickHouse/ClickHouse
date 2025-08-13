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

void Quota::doReplaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids)
{
    to_roles.replaceDependencies(old_to_new_ids);
}

}
