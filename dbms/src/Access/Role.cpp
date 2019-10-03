#include <Access/Role.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ROLE_NOT_FOUND;
    extern const int ROLE_ALREADY_EXISTS;
}


namespace AccessControlNames
{
    extern const size_t ROLE_NAMESPACE_IDX;
}


const Role::Type Role::TYPE{
    "Role", AccessControlNames::ROLE_NAMESPACE_IDX, nullptr, nullptr, ErrorCodes::ROLE_NOT_FOUND, ErrorCodes::ROLE_ALREADY_EXISTS};


bool Role::equal(const IAttributes & other) const
{
    if (!IAttributes::equal(other))
        return false;
    const auto & other_role = *other.cast<Role>();
    return (allowed_databases_by_grant_option[false] == other_role.allowed_databases_by_grant_option[false])
        && (allowed_databases_by_grant_option[true] == other_role.allowed_databases_by_grant_option[true])
        && (granted_roles_by_admin_option[false] == other_role.granted_roles_by_admin_option[false])
        && (granted_roles_by_admin_option[true] == other_role.granted_roles_by_admin_option[true]);
}


bool Role::hasReferences(const UUID & id) const
{
    return granted_roles_by_admin_option[false].count(id) || granted_roles_by_admin_option[true].count(id);
}


void Role::removeReferences(const UUID & id)
{
    granted_roles_by_admin_option[false].erase(id);
    granted_roles_by_admin_option[true].erase(id);
}
}
