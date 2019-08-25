#include <Account/RoleAttributes.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_CAST;
}


std::shared_ptr<IAccessAttributes> RoleAttributes::clone() const
{
    auto result = std::make_shared<RoleAttributes>();
    copyTo(*result);
    return result;
}


void RoleAttributes::copyTo(RoleAttributes & dest) const
{
    IAccessAttributes::copyTo(dest);
    dest.privileges = privileges;
    dest.grant_options = grant_options;
    dest.granted_roles = granted_roles;
    dest.applied_rls_policies = applied_rls_policies;
}


bool RoleAttributes::isEqual(const IAccessAttributes & other) const
{
    if (!IAccessAttributes::isEqual(other))
        return false;
    const auto & o = static_cast<const RoleAttributes &>(other);
    return (privileges == o.privileges) && (grant_options == o.grant_options) && (granted_roles == o.granted_roles)
        && (applied_rls_policies == o.applied_rls_policies);
}


template <>
RoleAttributes & IAccessAttributes::as<RoleAttributes>()
{
    if ((type == Type::ROLE) || (type == Type::USER))
        return static_cast<RoleAttributes &>(*this);
    throw Exception("Access attributes has unexpected type", ErrorCodes::BAD_CAST);
}

template <>
const RoleAttributes & IAccessAttributes::as<RoleAttributes>() const
{
    if ((type == Type::ROLE) || (type == Type::USER))
        return static_cast<const RoleAttributes &>(*this);
    throw Exception("Access attributes has unexpected type", ErrorCodes::BAD_CAST);
}

}
