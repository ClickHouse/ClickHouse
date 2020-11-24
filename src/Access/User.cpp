#include <Access/User.h>


namespace DB
{

bool User::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_user = typeid_cast<const User &>(other);
    return (authentication == other_user.authentication) && (allowed_client_hosts == other_user.allowed_client_hosts)
        && (access == other_user.access) && (granted_roles == other_user.granted_roles) && (default_roles == other_user.default_roles)
        && (settings == other_user.settings);
}

UserEtcCache & UserEtcCache::operator= (const UserEtcCache & other)
{
    std::scoped_lock lock(mutex, other.mutex);
    ldap_last_successful_password_check_params_hash = other.ldap_last_successful_password_check_params_hash;
    ldap_last_successful_password_check_timestamp = other.ldap_last_successful_password_check_timestamp;
    return *this;
}

UserEtcCache & UserEtcCache::operator= (UserEtcCache && other)
{
    std::scoped_lock lock(mutex, other.mutex);
    ldap_last_successful_password_check_params_hash = std::move(other.ldap_last_successful_password_check_params_hash);
    ldap_last_successful_password_check_timestamp = std::move(other.ldap_last_successful_password_check_timestamp);
    return *this;
}

}
