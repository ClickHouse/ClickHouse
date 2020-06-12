#pragma once

#include <Access/MultipleAccessStorage.h>
#include <Poco/AutoPtr.h>
#include <boost/container/flat_set.hpp>
#include <memory>


namespace Poco
{
    namespace Net
    {
        class IPAddress;
    }
    namespace Util
    {
        class AbstractConfiguration;
    }
}

namespace DB
{
class ContextAccess;
struct User;
using UserPtr = std::shared_ptr<const User>;
class EnabledRoles;
class RoleCache;
class EnabledRowPolicies;
class RowPolicyCache;
class EnabledQuota;
class QuotaCache;
struct QuotaUsage;
struct SettingsProfile;
using SettingsProfilePtr = std::shared_ptr<const SettingsProfile>;
class EnabledSettings;
class SettingsProfilesCache;
class SettingsProfileElements;
class ClientInfo;
struct Settings;


/// Manages access control entities.
class AccessControlManager : public MultipleAccessStorage
{
public:
    AccessControlManager();
    ~AccessControlManager();

    void setLocalDirectory(const String & directory);
    void setUsersConfig(const Poco::Util::AbstractConfiguration & users_config);
    void setDefaultProfileName(const String & default_profile_name);

    std::shared_ptr<const ContextAccess> getContextAccess(
        const UUID & user_id,
        const boost::container::flat_set<UUID> & current_roles,
        bool use_default_roles,
        const Settings & settings,
        const String & current_database,
        const ClientInfo & client_info) const;

    std::shared_ptr<const EnabledRoles> getEnabledRoles(
        const boost::container::flat_set<UUID> & current_roles,
        const boost::container::flat_set<UUID> & current_roles_with_admin_option) const;

    std::shared_ptr<const EnabledRowPolicies> getEnabledRowPolicies(
        const UUID & user_id,
        const boost::container::flat_set<UUID> & enabled_roles) const;

    std::shared_ptr<const EnabledQuota> getEnabledQuota(
        const UUID & user_id,
        const String & user_name,
        const boost::container::flat_set<UUID> & enabled_roles,
        const Poco::Net::IPAddress & address,
        const String & custom_quota_key) const;

    std::vector<QuotaUsage> getAllQuotasUsage() const;

    std::shared_ptr<const EnabledSettings> getEnabledSettings(const UUID & user_id,
                                                              const SettingsProfileElements & settings_from_user,
                                                              const boost::container::flat_set<UUID> & enabled_roles,
                                                              const SettingsProfileElements & settings_from_enabled_roles) const;

    std::shared_ptr<const SettingsChanges> getProfileSettings(const String & profile_name) const;

private:
    class ContextAccessCache;
    std::unique_ptr<ContextAccessCache> context_access_cache;
    std::unique_ptr<RoleCache> role_cache;
    std::unique_ptr<RowPolicyCache> row_policy_cache;
    std::unique_ptr<QuotaCache> quota_cache;
    std::unique_ptr<SettingsProfilesCache> settings_profiles_cache;
};

}
