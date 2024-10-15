#include <Access/User.h>
#include <Common/StringUtils.h>
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
    return (authentication_methods == other_user.authentication_methods)
        && (allowed_client_hosts == other_user.allowed_client_hosts)
        && (access == other_user.access) && (granted_roles == other_user.granted_roles) && (default_roles == other_user.default_roles)
        && (settings == other_user.settings) && (grantees == other_user.grantees) && (default_database == other_user.default_database)
        && (valid_until == other_user.valid_until);
}

void User::setName(const String & name_)
{
    /// Unfortunately, there is not way to distinguish USER_INTERSERVER_MARKER from actual username in native protocol,
    /// so we have to ensure that no such user will appear.
    /// Also it was possible to create a user with empty name for some reason.
    if (name_.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "User name is empty");
    if (name_ == EncodedUserInfo::USER_INTERSERVER_MARKER)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "User name '{}' is reserved", name_);
    if (name_.starts_with(EncodedUserInfo::SSH_KEY_AUTHENTICAION_MARKER))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "User name '{}' is reserved", name_);
    if (name_.starts_with(EncodedUserInfo::JWT_AUTHENTICAION_MARKER))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "User name '{}' is reserved", name_);
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

bool User::hasDependencies(const std::unordered_set<UUID> & ids) const
{
    return default_roles.hasDependencies(ids) || granted_roles.hasDependencies(ids) || grantees.hasDependencies(ids) || settings.hasDependencies(ids);
}

void User::replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids)
{
    default_roles.replaceDependencies(old_to_new_ids);
    granted_roles.replaceDependencies(old_to_new_ids);
    grantees.replaceDependencies(old_to_new_ids);
    settings.replaceDependencies(old_to_new_ids);
}

void User::copyDependenciesFrom(const IAccessEntity & src, const std::unordered_set<UUID> & ids)
{
    if (getType() != src.getType())
        return;
    const auto & src_user = typeid_cast<const User &>(src);
    default_roles.copyDependenciesFrom(src_user.default_roles, ids);
    granted_roles.copyDependenciesFrom(src_user.granted_roles, ids);
    grantees.copyDependenciesFrom(src_user.grantees, ids);
    settings.copyDependenciesFrom(src_user.settings, ids);
}

void User::removeDependencies(const std::unordered_set<UUID> & ids)
{
    default_roles.removeDependencies(ids);
    granted_roles.removeDependencies(ids);
    grantees.removeDependencies(ids);
    settings.removeDependencies(ids);
}

void User::clearAllExceptDependencies()
{
    authentication_methods.clear();
    allowed_client_hosts = AllowedClientHosts::AnyHostTag{};
    access = {};
    settings.removeSettingsKeepProfiles();
    default_database = {};
    valid_until = 0;
}

}
