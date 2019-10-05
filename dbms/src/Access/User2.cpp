#include <Access/User2.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int USER_NOT_FOUND;
    extern const int USER_ALREADY_EXISTS;
}


namespace AccessControlNames
{
    extern const size_t ROLE_NAMESPACE_IDX;
}


const User2::Type User2::TYPE{"User",
                              AccessControlNames::ROLE_NAMESPACE_IDX,
                              &Role::TYPE,
                              ErrorCodes::USER_NOT_FOUND,
                              ErrorCodes::USER_ALREADY_EXISTS};


bool User2::equal(const IAttributes & other) const
{
    if (!Role::equal(other))
        return false;
    const auto & other_user = *other.cast<User2>();
    return (authentication == other_user.authentication) && (allowed_hosts == other_user.allowed_hosts)
        && (default_roles == other_user.default_roles) && (settings == other_user.settings)
        && (settings_constraints == other_user.settings_constraints) && (account_locked == other_user.account_locked);
}


bool User2::hasReferences(const UUID & id) const
{
    return Role::hasReferences(id);
}


void User2::removeReferences(const UUID & id)
{
    Role::removeReferences(id);
}
}
