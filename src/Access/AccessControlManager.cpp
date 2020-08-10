#include <Access/AccessControlManager.h>
#include <Access/MultipleAccessStorage.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Access/DiskAccessStorage.h>
#include <Access/ContextAccess.h>
#include <Access/RoleCache.h>
#include <Access/RowPolicyCache.h>
#include <Access/QuotaCache.h>
#include <Access/QuotaUsage.h>
#include <Access/SettingsProfilesCache.h>
#include <Access/ExternalAuthenticators.h>
#include <Core/Settings.h>
#include <common/find_symbols.h>
#include <Poco/ExpireCache.h>
#include <boost/algorithm/string/join.hpp>
#include <mutex>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

namespace
{
    std::vector<std::unique_ptr<IAccessStorage>> createStorages()
    {
        std::vector<std::unique_ptr<IAccessStorage>> list;
        list.emplace_back(std::make_unique<DiskAccessStorage>());
        list.emplace_back(std::make_unique<UsersConfigAccessStorage>());

#if 0  /// Memory access storage is disabled.
        list.emplace_back(std::make_unique<MemoryAccessStorage>());
#endif
        return list;
    }

    constexpr size_t DISK_ACCESS_STORAGE_INDEX = 0;
    constexpr size_t USERS_CONFIG_ACCESS_STORAGE_INDEX = 1;
}


class AccessControlManager::ContextAccessCache
{
public:
    explicit ContextAccessCache(const AccessControlManager & manager_) : manager(manager_) {}

    std::shared_ptr<const ContextAccess> getContextAccess(const ContextAccessParams & params)
    {
        std::lock_guard lock{mutex};
        auto x = cache.get(params);
        if (x)
            return *x;
        auto res = std::shared_ptr<ContextAccess>(new ContextAccess(manager, params));
        cache.add(params, res);
        return res;
    }

private:
    const AccessControlManager & manager;
    Poco::ExpireCache<ContextAccess::Params, std::shared_ptr<const ContextAccess>> cache;
    std::mutex mutex;
};


class AccessControlManager::CustomSettingsPrefixes
{
public:
    void registerPrefixes(const Strings & prefixes_)
    {
        std::lock_guard lock{mutex};
        registered_prefixes = prefixes_;
    }

    bool isSettingNameAllowed(const std::string_view & setting_name) const
    {
        if (Settings::hasBuiltin(setting_name))
            return true;

        std::lock_guard lock{mutex};
        for (const auto & prefix : registered_prefixes)
        {
            if (setting_name.starts_with(prefix))
                return true;
        }

        return false;
    }

    void checkSettingNameIsAllowed(const std::string_view & setting_name) const
    {
        if (isSettingNameAllowed(setting_name))
            return;

        std::lock_guard lock{mutex};
        if (!registered_prefixes.empty())
        {
            throw Exception(
                "Setting " + String{setting_name} + " is neither a builtin setting nor started with the prefix '"
                    + boost::algorithm::join(registered_prefixes, "' or '") + "' registered for user-defined settings",
                ErrorCodes::UNKNOWN_SETTING);
        }
        else
            BaseSettingsHelpers::throwSettingNotFound(setting_name);
    }

private:
    Strings registered_prefixes;
    mutable std::mutex mutex;
};


AccessControlManager::AccessControlManager()
    : MultipleAccessStorage(createStorages()),
      context_access_cache(std::make_unique<ContextAccessCache>(*this)),
      role_cache(std::make_unique<RoleCache>(*this)),
      row_policy_cache(std::make_unique<RowPolicyCache>(*this)),
      quota_cache(std::make_unique<QuotaCache>(*this)),
      settings_profiles_cache(std::make_unique<SettingsProfilesCache>(*this)),
      external_authenticators(std::make_unique<ExternalAuthenticators>()),
      custom_settings_prefixes(std::make_unique<CustomSettingsPrefixes>())
{
}


AccessControlManager::~AccessControlManager() = default;


void AccessControlManager::setLocalDirectory(const String & directory_path)
{
    auto & disk_access_storage = dynamic_cast<DiskAccessStorage &>(getStorageByIndex(DISK_ACCESS_STORAGE_INDEX));
    disk_access_storage.setDirectory(directory_path);
}


void AccessControlManager::setExternalAuthenticatorsConfig(const Poco::Util::AbstractConfiguration & config)
{
    external_authenticators->setConfig(config, getLogger());
}


void AccessControlManager::setUsersConfig(const Poco::Util::AbstractConfiguration & users_config)
{
    auto & users_config_access_storage = dynamic_cast<UsersConfigAccessStorage &>(getStorageByIndex(USERS_CONFIG_ACCESS_STORAGE_INDEX));
    users_config_access_storage.setConfiguration(users_config);
}


void AccessControlManager::setDefaultProfileName(const String & default_profile_name)
{
    settings_profiles_cache->setDefaultProfileName(default_profile_name);
}


void AccessControlManager::setCustomSettingsPrefixes(const Strings & prefixes)
{
    custom_settings_prefixes->registerPrefixes(prefixes);
}

void AccessControlManager::setCustomSettingsPrefixes(const String & comma_separated_prefixes)
{
    Strings prefixes;
    splitInto<','>(prefixes, comma_separated_prefixes);
    setCustomSettingsPrefixes(prefixes);
}

bool AccessControlManager::isSettingNameAllowed(const std::string_view & setting_name) const
{
    return custom_settings_prefixes->isSettingNameAllowed(setting_name);
}

void AccessControlManager::checkSettingNameIsAllowed(const std::string_view & setting_name) const
{
    custom_settings_prefixes->checkSettingNameIsAllowed(setting_name);
}


std::shared_ptr<const ContextAccess> AccessControlManager::getContextAccess(
    const UUID & user_id,
    const boost::container::flat_set<UUID> & current_roles,
    bool use_default_roles,
    const Settings & settings,
    const String & current_database,
    const ClientInfo & client_info) const
{
    ContextAccessParams params;
    params.user_id = user_id;
    params.current_roles = current_roles;
    params.use_default_roles = use_default_roles;
    params.current_database = current_database;
    params.readonly = settings.readonly;
    params.allow_ddl = settings.allow_ddl;
    params.allow_introspection = settings.allow_introspection_functions;
    params.interface = client_info.interface;
    params.http_method = client_info.http_method;
    params.address = client_info.current_address.host();
    params.quota_key = client_info.quota_key;
    return getContextAccess(params);
}


std::shared_ptr<const ContextAccess> AccessControlManager::getContextAccess(const ContextAccessParams & params) const
{
    return context_access_cache->getContextAccess(params);
}


std::shared_ptr<const EnabledRoles> AccessControlManager::getEnabledRoles(
    const boost::container::flat_set<UUID> & current_roles,
    const boost::container::flat_set<UUID> & current_roles_with_admin_option) const
{
    return role_cache->getEnabledRoles(current_roles, current_roles_with_admin_option);
}


std::shared_ptr<const EnabledRowPolicies> AccessControlManager::getEnabledRowPolicies(const UUID & user_id, const boost::container::flat_set<UUID> & enabled_roles) const
{
    return row_policy_cache->getEnabledRowPolicies(user_id, enabled_roles);
}


std::shared_ptr<const EnabledQuota> AccessControlManager::getEnabledQuota(
    const UUID & user_id, const String & user_name, const boost::container::flat_set<UUID> & enabled_roles, const Poco::Net::IPAddress & address, const String & custom_quota_key) const
{
    return quota_cache->getEnabledQuota(user_id, user_name, enabled_roles, address, custom_quota_key);
}


std::vector<QuotaUsage> AccessControlManager::getAllQuotasUsage() const
{
    return quota_cache->getAllQuotasUsage();
}


std::shared_ptr<const EnabledSettings> AccessControlManager::getEnabledSettings(
    const UUID & user_id,
    const SettingsProfileElements & settings_from_user,
    const boost::container::flat_set<UUID> & enabled_roles,
    const SettingsProfileElements & settings_from_enabled_roles) const
{
    return settings_profiles_cache->getEnabledSettings(user_id, settings_from_user, enabled_roles, settings_from_enabled_roles);
}

std::shared_ptr<const SettingsChanges> AccessControlManager::getProfileSettings(const String & profile_name) const
{
    return settings_profiles_cache->getProfileSettings(profile_name);
}

const ExternalAuthenticators & AccessControlManager::getExternalAuthenticators() const
{
    return *external_authenticators;
}

}
