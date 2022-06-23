#include <Access/User.h>
#include <Core/Protocol.h>
#include <base/insertAtEnd.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

bool User::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_user = typeid_cast<const User &>(other);
    return (auth_data == other_user.auth_data) && (allowed_client_hosts == other_user.allowed_client_hosts)
        && (access == other_user.access) && (granted_roles == other_user.granted_roles) && (default_roles == other_user.default_roles)
        && (settings == other_user.settings) && (grantees == other_user.grantees) && (default_database == other_user.default_database);
}

void User::setName(const String & name_)
{
    /// Unfortunately, there is not way to distinguish USER_INTERSERVER_MARKER from actual username in native protocol,
    /// so we have to ensure that no such user will appear.
    /// Also it was possible to create a user with empty name for some reason.
    if (name_.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "User name is empty");
    if (name_ == USER_INTERSERVER_MARKER)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "User name '{}' is reserved", USER_INTERSERVER_MARKER);
    name = name_;
}

std::vector<UUID> User::findDependencies() const
{
    std::vector<UUID> res;
    insertAtEnd(res, default_roles.findDependencies());
    insertAtEnd(res, granted_roles.findDependencies());
    insertAtEnd(res, grantees.findDependencies());
    insertAtEnd(res, settings.findDependencies());
    return res;
}

void User::replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids)
{
    default_roles.replaceDependencies(old_to_new_ids);
    granted_roles.replaceDependencies(old_to_new_ids);
    grantees.replaceDependencies(old_to_new_ids);
    settings.replaceDependencies(old_to_new_ids);
}

}
