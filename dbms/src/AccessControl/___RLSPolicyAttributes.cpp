#if 0
#include <AccessControl/RLSPolicyAttributes.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_CAST;
}


std::shared_ptr<IAccessAttributes> RLSPolicyAttributes::clone() const
{
    auto result = std::make_shared<RLSPolicyAttributes>();
    copyTo(*result);
    return result;
}


void RLSPolicyAttributes::copyTo(RLSPolicyAttributes & dest) const
{
    IAccessAttributes::copyTo(dest);
    dest.database = database;
    dest.table = table;
    dest.policy_type = policy_type;
    dest.select_filter = select_filter;
    dest.insert_filter = insert_filter;
    dest.apply_to_roles = apply_to_roles;
}


bool RLSPolicyAttributes::isEqual(const IAccessAttributes & other) const
{
    if (!IAccessAttributes::isEqual(other))
        return false;
    const auto & o = static_cast<const RLSPolicyAttributes &>(other);
    return (database == o.database) && (table == o.table) && (policy_type == o.policy_type) && (select_filter == o.select_filter)
        && (insert_filter == o.insert_filter) && (apply_to_roles == o.apply_to_roles);
}


template <>
RLSPolicyAttributes & IAccessAttributes::as<RLSPolicyAttributes>()
{
    if (type == Type::RLS_POLICY)
        return static_cast<RLSPolicyAttributes &>(*this);
    throw Exception("Access attributes has unexpected type", ErrorCodes::BAD_CAST);
}

template <>
const RLSPolicyAttributes & IAccessAttributes::as<RLSPolicyAttributes>() const
{
    if (type == Type::RLS_POLICY)
        return static_cast<const RLSPolicyAttributes &>(*this);
    throw Exception("Access attributes has unexpected type", ErrorCodes::BAD_CAST);
}
}
#endif
