#include <Access/Role.h>
#include <algorithm>


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
    "Role", AccessControlNames::ROLE_NAMESPACE_IDX, nullptr, ErrorCodes::ROLE_NOT_FOUND, ErrorCodes::ROLE_ALREADY_EXISTS};


bool Role::equal(const IAttributes & other) const
{
    if (!IAttributes::equal(other))
        return false;
    const auto & other_role = *other.cast<Role>();
    return std::equal(std::begin(privileges), std::end(privileges), std::begin(other_role.privileges))
        && std::equal(std::begin(granted_roles), std::end(granted_roles), std::begin(other_role.granted_roles));
}


bool Role::hasReferences(const UUID & id) const
{
    return granted_roles[false].count(id) || granted_roles[true].count(id);
}


void Role::removeReferences(const UUID & id)
{
    granted_roles[false].erase(id);
    granted_roles[true].erase(id);
}
}
