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
#include <Poco/ExpireCache.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <mutex>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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

    auto parseLDAPServer(const Poco::Util::AbstractConfiguration & config, const String & ldap_server_name)
    {
        LDAPServerParams params;

        const String ldap_server_config = "ldap_servers." + ldap_server_name;

        const bool has_host = config.has(ldap_server_config + ".host");
        const bool has_port = config.has(ldap_server_config + ".port");
        const bool has_auth_dn_prefix = config.has(ldap_server_config + ".auth_dn_prefix");
        const bool has_auth_dn_suffix = config.has(ldap_server_config + ".auth_dn_suffix");
        const bool has_enable_tls = config.has(ldap_server_config + ".enable_tls");
        const bool has_tls_cert_verify = config.has(ldap_server_config + ".tls_cert_verify");
        const bool has_ca_cert_dir = config.has(ldap_server_config + ".ca_cert_dir");
        const bool has_ca_cert_file = config.has(ldap_server_config + ".ca_cert_file");

        if (!has_host)
            throw Exception("Missing 'host' entry", ErrorCodes::BAD_ARGUMENTS);

        params.host = config.getString(ldap_server_config + ".host");

        if (params.host.empty())
            throw Exception("Empty 'host' entry", ErrorCodes::BAD_ARGUMENTS);

        if (has_auth_dn_prefix)
            params.auth_dn_prefix = config.getString(ldap_server_config + ".auth_dn_prefix");

        if (has_auth_dn_suffix)
            params.auth_dn_suffix = config.getString(ldap_server_config + ".auth_dn_suffix");

        if (has_enable_tls)
        {
            String enable_tls_lc_str = config.getString(ldap_server_config + ".enable_tls");
            boost::to_lower(enable_tls_lc_str);

            if (enable_tls_lc_str == "starttls")
                params.enable_tls = LDAPServerParams::TLSEnable::YES_STARTTLS;
            else if (config.getBool(ldap_server_config + ".enable_tls"))
                params.enable_tls = LDAPServerParams::TLSEnable::YES;
            else
                params.enable_tls = LDAPServerParams::TLSEnable::NO;
        }

        if (has_tls_cert_verify)
        {
            String tls_cert_verify_lc_str = config.getString(ldap_server_config + ".tls_cert_verify");
            boost::to_lower(tls_cert_verify_lc_str);

            if (tls_cert_verify_lc_str == "never")
                params.tls_cert_verify = LDAPServerParams::TLSCertVerify::NEVER;
            else if (tls_cert_verify_lc_str == "allow")
                params.tls_cert_verify = LDAPServerParams::TLSCertVerify::ALLOW;
            else if (tls_cert_verify_lc_str == "try")
                params.tls_cert_verify = LDAPServerParams::TLSCertVerify::TRY;
            else if (tls_cert_verify_lc_str == "demand")
                params.tls_cert_verify = LDAPServerParams::TLSCertVerify::DEMAND;
            else
                throw Exception("Bad value for 'tls_cert_verify' entry, allowed values are: 'never', 'allow', 'try', 'demand'", ErrorCodes::BAD_ARGUMENTS);
        }

        if (has_ca_cert_dir)
            params.ca_cert_dir = config.getString(ldap_server_config + ".ca_cert_dir");

        if (has_ca_cert_file)
            params.ca_cert_file = config.getString(ldap_server_config + ".ca_cert_file");

        if (has_port)
        {
            const auto port = config.getInt64(ldap_server_config + ".port");
            if (port < 0 || port > 65535)
                throw Exception("Bad value for 'port' entry", ErrorCodes::BAD_ARGUMENTS);

            params.port = port;
        }
        else
            params.port = (params.enable_tls == LDAPServerParams::TLSEnable::YES ? 636 : 389);

        return params;
    }

    void parseAndAddLDAPServers(ExternalAuthenticators & external_authenticators, const Poco::Util::AbstractConfiguration & config, Poco::Logger * log)
    {
        Poco::Util::AbstractConfiguration::Keys ldap_server_names;
        config.keys("ldap_servers", ldap_server_names);

        for (const auto & ldap_server_name : ldap_server_names)
        {
            try
            {
                external_authenticators.setLDAPServerParams(ldap_server_name, parseLDAPServer(config, ldap_server_name));
            }
            catch (...)
            {
                tryLogCurrentException(log, "Could not parse LDAP server " + backQuote(ldap_server_name));
            }
        }
    }

    auto parseExternalAuthenticators(const Poco::Util::AbstractConfiguration & config, Poco::Logger * log)
    {
        auto external_authenticators = std::make_unique<ExternalAuthenticators>();
        parseAndAddLDAPServers(*external_authenticators, config, log);
        return external_authenticators;
    }
}


class AccessControlManager::ContextAccessCache
{
public:
    explicit ContextAccessCache(const AccessControlManager & manager_) : manager(manager_) {}

    std::shared_ptr<const ContextAccess> getContextAccess(
        const UUID & user_id,
        const boost::container::flat_set<UUID> & current_roles,
        bool use_default_roles,
        const Settings & settings,
        const String & current_database,
        const ClientInfo & client_info)
    {
        ContextAccess::Params params;
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


AccessControlManager::AccessControlManager()
    : MultipleAccessStorage(createStorages()),
      context_access_cache(std::make_unique<ContextAccessCache>(*this)),
      role_cache(std::make_unique<RoleCache>(*this)),
      row_policy_cache(std::make_unique<RowPolicyCache>(*this)),
      quota_cache(std::make_unique<QuotaCache>(*this)),
      settings_profiles_cache(std::make_unique<SettingsProfilesCache>(*this))
{
}


AccessControlManager::~AccessControlManager() = default;


void AccessControlManager::setLocalDirectory(const String & directory_path)
{
    auto & disk_access_storage = dynamic_cast<DiskAccessStorage &>(getStorageByIndex(DISK_ACCESS_STORAGE_INDEX));
    disk_access_storage.setDirectory(directory_path);
}


void AccessControlManager::setUsersConfig(const Poco::Util::AbstractConfiguration & users_config)
{
    external_authenticators = parseExternalAuthenticators(users_config, getLogger());
    auto & users_config_access_storage = dynamic_cast<UsersConfigAccessStorage &>(getStorageByIndex(USERS_CONFIG_ACCESS_STORAGE_INDEX));
    users_config_access_storage.setConfiguration(users_config);
}


void AccessControlManager::setDefaultProfileName(const String & default_profile_name)
{
    settings_profiles_cache->setDefaultProfileName(default_profile_name);
}


std::shared_ptr<const ContextAccess> AccessControlManager::getContextAccess(
    const UUID & user_id,
    const boost::container::flat_set<UUID> & current_roles,
    bool use_default_roles,
    const Settings & settings,
    const String & current_database,
    const ClientInfo & client_info) const
{
    return context_access_cache->getContextAccess(user_id, current_roles, use_default_roles, settings, current_database, client_info);
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
