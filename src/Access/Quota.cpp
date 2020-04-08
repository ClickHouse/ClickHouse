#include <Access/Quota.h>
#include <boost/range/algorithm/equal.hpp>
#include <boost/range/algorithm/fill.hpp>


namespace DB
{
Quota::Limits::Limits()
{
    boost::range::fill(max, 0);
}


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
    return (all_limits == other_quota.all_limits) && (key_type == other_quota.key_type) && (roles == other_quota.roles)
        && (all_roles == other_quota.all_roles) && (except_roles == other_quota.except_roles);
}


const char * Quota::resourceTypeToColumnName(ResourceType resource_type)
{
    switch (resource_type)
    {
        case Quota::QUERIES: return "queries";
        case Quota::ERRORS: return "errors";
        case Quota::RESULT_ROWS: return "result_rows";
        case Quota::RESULT_BYTES: return "result_bytes";
        case Quota::READ_ROWS: return "read_rows";
        case Quota::READ_BYTES: return "read_bytes";
        case Quota::EXECUTION_TIME: return "execution_time";
    }
    __builtin_unreachable();
}
}

