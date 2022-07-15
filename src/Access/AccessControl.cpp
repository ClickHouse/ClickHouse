#include <Access/AccessControl.h>
#include <Access/MultipleAccessStorage.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/ReplicatedAccessStorage.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Access/DiskAccessStorage.h>
#include <Access/LDAPAccessStorage.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/RoleCache.h>
#include <Access/RowPolicyCache.h>
#include <Access/QuotaCache.h>
#include <Access/QuotaUsage.h>
#include <Access/SettingsProfilesCache.h>
#include <Access/User.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/AccessChangesNotifier.h>
#include <Access/AccessBackup.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/RestorerFromBackup.h>
#include <Core/Settings.h>
#include <base/defines.h>
#include <base/find_symbols.h>
#include <Poco/AccessExpireCache.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <filesystem>
#include <mutex>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_SETTING;
    extern const int AUTHENTICATION_FAILED;
}


namespace
{
    void checkForUsersNotInMainConfig(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_path,
        const std::string & users_config_path,
        Poco::Logger * log)
    {
        if (config.getBool("skip_check_for_incorrect_settings", false))
            return;

        if (config.has("users") || config.has("profiles") || config.has("quotas"))
        {
            /// We cannot throw exception here, because we have support for obsolete 'conf.d' directory
            /// (that does not correspond to config.d or users.d) but substitute configuration to both of them.

            LOG_ERROR(log, "The <users>, <profiles> and <quotas> elements should be located in users config file: {} not in main config {}."
                " Also note that you should place configuration changes to the appropriate *.d directory like 'users.d'.",
                users_config_path, config_path);
        }
    }
}


class AccessControl::ContextAccessCache
{
public:
    explicit ContextAccessCache(const AccessControl & access_control_) : access_control(access_control_) {}

    std::shared_ptr<const ContextAccess> getContextAccess(const ContextAccessParams & params)
    {
        std::lock_guard lock{mutex};
        auto x = cache.get(params);
        if (x)
        {
            if ((*x)->tryGetUser())
                return *x;
            /// No user, probably the user has been dropped while it was in the cache.
            cache.remove(params);
        }
        auto res = std::shared_ptr<ContextAccess>(new ContextAccess(access_control, params));
        res->initialize();
        cache.add(params, res);
        return res;
    }

private:
    const AccessControl & access_control;
    Poco::AccessExpireCache<ContextAccess::Params, std::shared_ptr<const ContextAccess>> cache;
    std::mutex mutex;
};


class AccessControl::CustomSettingsPrefixes
{
public:
    void registerPrefixes(const Strings & prefixes_)
    {
        std::lock_guard lock{mutex};
        registered_prefixes = prefixes_;
    }

    bool isSettingNameAllowed(std::string_view setting_name) const
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

    void checkSettingNameIsAllowed(std::string_view setting_name) const
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
    Strings registered_prefixes TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;
};


AccessControl::AccessControl()
    : MultipleAccessStorage("user directories"),
      context_access_cache(std::make_unique<ContextAccessCache>(*this)),
      role_cache(std::make_unique<RoleCache>(*this)),
      row_policy_cache(std::make_unique<RowPolicyCache>(*this)),
      quota_cache(std::make_unique<QuotaCache>(*this)),
      settings_profiles_cache(std::make_unique<SettingsProfilesCache>(*this)),
      external_authenticators(std::make_unique<ExternalAuthenticators>()),
      custom_settings_prefixes(std::make_unique<CustomSettingsPrefixes>()),
      changes_notifier(std::make_unique<AccessChangesNotifier>())
{
}


AccessControl::~AccessControl() = default;


void AccessControl::setUpFromMainConfig(const Poco::Util::AbstractConfiguration & config_, const String & config_path_,
                                        const zkutil::GetZooKeeper & get_zookeeper_function_)
{
    if (config_.has("custom_settings_prefixes"))
        setCustomSettingsPrefixes(config_.getString("custom_settings_prefixes"));

    setNoPasswordAllowed(config_.getBool("allow_no_password", true));
    setPlaintextPasswordAllowed(config_.getBool("allow_plaintext_password", true));

    setEnabledUsersWithoutRowPoliciesCanReadRows(config_.getBool(
        "access_control_improvements.users_without_row_policies_can_read_rows",
        false /* false because we need to be compatible with earlier access configurations */));

    setOnClusterQueriesRequireClusterGrant(config_.getBool(
        "access_control_improvements.on_cluster_queries_require_cluster_grant",
        false /* false because we need to be compatible with earlier access configurations */));

    addStoragesFromMainConfig(config_, config_path_, get_zookeeper_function_);
}


void AccessControl::setUsersConfig(const Poco::Util::AbstractConfiguration & users_config_)
{
    auto storages = getStoragesPtr();
    for (const auto & storage : *storages)
    {
        if (auto users_config_storage = typeid_cast<std::shared_ptr<UsersConfigAccessStorage>>(storage))
        {
            users_config_storage->setConfig(users_config_);
            return;
        }
    }
    addUsersConfigStorage(UsersConfigAccessStorage::STORAGE_TYPE, users_config_, false);
}

void AccessControl::addUsersConfigStorage(const String & storage_name_, const Poco::Util::AbstractConfiguration & users_config_, bool allow_backup_)
{
    auto new_storage = std::make_shared<UsersConfigAccessStorage>(storage_name_, *this, allow_backup_);
    new_storage->setConfig(users_config_);
    addStorage(new_storage);
    LOG_DEBUG(getLogger(), "Added {} access storage '{}', path: {}",
        String(new_storage->getStorageType()), new_storage->getStorageName(), new_storage->getPath());
}

void AccessControl::addUsersConfigStorage(
    const String & storage_name_,
    const String & users_config_path_,
    const String & include_from_path_,
    const String & preprocessed_dir_,
    const zkutil::GetZooKeeper & get_zookeeper_function_,
    bool allow_backup_)
{
    auto storages = getStoragesPtr();
    for (const auto & storage : *storages)
    {
        if (auto users_config_storage = typeid_cast<std::shared_ptr<UsersConfigAccessStorage>>(storage))
        {
            if (users_config_storage->isPathEqual(users_config_path_))
                return;
        }
    }
    auto new_storage = std::make_shared<UsersConfigAccessStorage>(storage_name_, *this, allow_backup_);
    new_storage->load(users_config_path_, include_from_path_, preprocessed_dir_, get_zookeeper_function_);
    addStorage(new_storage);
    LOG_DEBUG(getLogger(), "Added {} access storage '{}', path: {}", String(new_storage->getStorageType()), new_storage->getStorageName(), new_storage->getPath());
}


void AccessControl::addReplicatedStorage(
    const String & storage_name_,
    const String & zookeeper_path_,
    const zkutil::GetZooKeeper & get_zookeeper_function_,
    bool allow_backup_)
{
    auto storages = getStoragesPtr();
    for (const auto & storage : *storages)
    {
        if (auto replicated_storage = typeid_cast<std::shared_ptr<ReplicatedAccessStorage>>(storage))
            return;
    }
    auto new_storage = std::make_shared<ReplicatedAccessStorage>(storage_name_, zookeeper_path_, get_zookeeper_function_, *changes_notifier, allow_backup_);
    addStorage(new_storage);
    LOG_DEBUG(getLogger(), "Added {} access storage '{}'", String(new_storage->getStorageType()), new_storage->getStorageName());
}

void AccessControl::addDiskStorage(const String & storage_name_, const String & directory_, bool readonly_, bool allow_backup_)
{
    auto storages = getStoragesPtr();
    for (const auto & storage : *storages)
    {
        if (auto disk_storage = typeid_cast<std::shared_ptr<DiskAccessStorage>>(storage))
        {
            if (disk_storage->isPathEqual(directory_))
            {
                if (readonly_)
                    disk_storage->setReadOnly(readonly_);
                return;
            }
        }
    }
    auto new_storage = std::make_shared<DiskAccessStorage>(storage_name_, directory_, *changes_notifier, readonly_, allow_backup_);
    addStorage(new_storage);
    LOG_DEBUG(getLogger(), "Added {} access storage '{}', path: {}", String(new_storage->getStorageType()), new_storage->getStorageName(), new_storage->getPath());
}


void AccessControl::addMemoryStorage(const String & storage_name_, bool allow_backup_)
{
    auto storages = getStoragesPtr();
    for (const auto & storage : *storages)
    {
        if (auto memory_storage = typeid_cast<std::shared_ptr<MemoryAccessStorage>>(storage))
            return;
    }
    auto new_storage = std::make_shared<MemoryAccessStorage>(storage_name_, *changes_notifier, allow_backup_);
    addStorage(new_storage);
    LOG_DEBUG(getLogger(), "Added {} access storage '{}'", String(new_storage->getStorageType()), new_storage->getStorageName());
}


void AccessControl::addLDAPStorage(const String & storage_name_, const Poco::Util::AbstractConfiguration & config_, const String & prefix_)
{
    auto new_storage = std::make_shared<LDAPAccessStorage>(storage_name_, *this, config_, prefix_);
    addStorage(new_storage);
    LOG_DEBUG(getLogger(), "Added {} access storage '{}', LDAP server name: {}", String(new_storage->getStorageType()), new_storage->getStorageName(), new_storage->getLDAPServerName());
}


void AccessControl::addStoragesFromUserDirectoriesConfig(
    const Poco::Util::AbstractConfiguration & config,
    const String & key,
    const String & config_dir,
    const String & dbms_dir,
    const String & include_from_path,
    const zkutil::GetZooKeeper & get_zookeeper_function)
{
    Strings keys_in_user_directories;
    config.keys(key, keys_in_user_directories);

    for (const String & key_in_user_directories : keys_in_user_directories)
    {
        String prefix = key + "." + key_in_user_directories;

        String type = key_in_user_directories;
        if (size_t bracket_pos = type.find('['); bracket_pos != String::npos)
            type.resize(bracket_pos);
        if ((type == "users_xml") || (type == "users_config"))
            type = UsersConfigAccessStorage::STORAGE_TYPE;
        else if ((type == "local") || (type == "local_directory"))
            type = DiskAccessStorage::STORAGE_TYPE;
        else if (type == "ldap")
            type = LDAPAccessStorage::STORAGE_TYPE;

        String name = config.getString(prefix + ".name", type);

        if (type == MemoryAccessStorage::STORAGE_TYPE)
        {
            bool allow_backup = config.getBool(prefix + ".allow_backup", true);
            addMemoryStorage(name, allow_backup);
        }
        else if (type == UsersConfigAccessStorage::STORAGE_TYPE)
        {
            String path = config.getString(prefix + ".path");
            if (std::filesystem::path{path}.is_relative() && std::filesystem::exists(config_dir + path))
                path = config_dir + path;
            bool allow_backup = config.getBool(prefix + ".allow_backup", false); /// We don't backup users.xml by default.
            addUsersConfigStorage(name, path, include_from_path, dbms_dir, get_zookeeper_function, allow_backup);
        }
        else if (type == DiskAccessStorage::STORAGE_TYPE)
        {
            String path = config.getString(prefix + ".path");
            bool readonly = config.getBool(prefix + ".readonly", false);
            bool allow_backup = config.getBool(prefix + ".allow_backup", true);
            addDiskStorage(name, path, readonly, allow_backup);
        }
        else if (type == LDAPAccessStorage::STORAGE_TYPE)
        {
            addLDAPStorage(name, config, prefix);
        }
        else if (type == ReplicatedAccessStorage::STORAGE_TYPE)
        {
            String zookeeper_path = config.getString(prefix + ".zookeeper_path");
            bool allow_backup = config.getBool(prefix + ".allow_backup", true);
            addReplicatedStorage(name, zookeeper_path, get_zookeeper_function, allow_backup);
        }
        else
            throw Exception("Unknown storage type '" + type + "' at " + prefix + " in config", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
    }
}


void AccessControl::addStoragesFromMainConfig(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_path,
    const zkutil::GetZooKeeper & get_zookeeper_function)
{
    String config_dir = std::filesystem::path{config_path}.remove_filename().string();
    String dbms_dir = config.getString("path", DBMS_DEFAULT_PATH);
    String include_from_path = config.getString("include_from", "/etc/metrika.xml");
    bool has_user_directories = config.has("user_directories");

    /// If path to users' config isn't absolute, try guess its root (current) dir.
    /// At first, try to find it in dir of main config, after will use current dir.
    String users_config_path = config.getString("users_config", "");
    if (users_config_path.empty())
    {
        if (!has_user_directories)
            users_config_path = config_path;
    }
    else if (std::filesystem::path{users_config_path}.is_relative() && std::filesystem::exists(config_dir + users_config_path))
        users_config_path = config_dir + users_config_path;

    if (!users_config_path.empty())
    {
        if (users_config_path != config_path)
            checkForUsersNotInMainConfig(config, config_path, users_config_path, getLogger());

        addUsersConfigStorage(
            UsersConfigAccessStorage::STORAGE_TYPE,
            users_config_path,
            include_from_path,
            dbms_dir,
            get_zookeeper_function,
            /* allow_backup= */ false);
    }

    String disk_storage_dir = config.getString("access_control_path", "");
    if (!disk_storage_dir.empty())
        addDiskStorage(DiskAccessStorage::STORAGE_TYPE, disk_storage_dir, /* readonly= */ false, /* allow_backup= */ true);

    if (has_user_directories)
        addStoragesFromUserDirectoriesConfig(config, "user_directories", config_dir, dbms_dir, include_from_path, get_zookeeper_function);
}


void AccessControl::reload()
{
    MultipleAccessStorage::reload();
    changes_notifier->sendNotifications();
}

scope_guard AccessControl::subscribeForChanges(AccessEntityType type, const OnChangedHandler & handler) const
{
    return changes_notifier->subscribeForChanges(type, handler);
}

scope_guard AccessControl::subscribeForChanges(const UUID & id, const OnChangedHandler & handler) const
{
    return changes_notifier->subscribeForChanges(id, handler);
}

scope_guard AccessControl::subscribeForChanges(const std::vector<UUID> & ids, const OnChangedHandler & handler) const
{
    return changes_notifier->subscribeForChanges(ids, handler);
}

std::optional<UUID> AccessControl::insertImpl(const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists)
{
    auto id = MultipleAccessStorage::insertImpl(entity, replace_if_exists, throw_if_exists);
    if (id)
        changes_notifier->sendNotifications();
    return id;
}

bool AccessControl::removeImpl(const UUID & id, bool throw_if_not_exists)
{
    bool removed = MultipleAccessStorage::removeImpl(id, throw_if_not_exists);
    if (removed)
        changes_notifier->sendNotifications();
    return removed;
}

bool AccessControl::updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    bool updated = MultipleAccessStorage::updateImpl(id, update_func, throw_if_not_exists);
    if (updated)
        changes_notifier->sendNotifications();
    return updated;
}

AccessChangesNotifier & AccessControl::getChangesNotifier()
{
    return *changes_notifier;
}


UUID AccessControl::authenticate(const Credentials & credentials, const Poco::Net::IPAddress & address) const
{
    try
    {
        return MultipleAccessStorage::authenticate(credentials, address, *external_authenticators, allow_no_password,
                                                   allow_plaintext_password);
    }
    catch (...)
    {
        tryLogCurrentException(getLogger(), "from: " + address.toString() + ", user: " + credentials.getUserName()  + ": Authentication failed");

        /// We use the same message for all authentication failures because we don't want to give away any unnecessary information for security reasons,
        /// only the log will show the exact reason.
        throw Exception(credentials.getUserName() + ": Authentication failed: password is incorrect or there is no user with such name", ErrorCodes::AUTHENTICATION_FAILED);
    }
}

void AccessControl::restoreFromBackup(RestorerFromBackup & restorer)
{
    MultipleAccessStorage::restoreFromBackup(restorer);
    changes_notifier->sendNotifications();
}

void AccessControl::setExternalAuthenticatorsConfig(const Poco::Util::AbstractConfiguration & config)
{
    external_authenticators->setConfiguration(config, getLogger());
}


void AccessControl::setDefaultProfileName(const String & default_profile_name)
{
    settings_profiles_cache->setDefaultProfileName(default_profile_name);
}


void AccessControl::setCustomSettingsPrefixes(const Strings & prefixes)
{
    custom_settings_prefixes->registerPrefixes(prefixes);
}

void AccessControl::setCustomSettingsPrefixes(const String & comma_separated_prefixes)
{
    Strings prefixes;
    splitInto<','>(prefixes, comma_separated_prefixes);
    setCustomSettingsPrefixes(prefixes);
}

bool AccessControl::isSettingNameAllowed(const std::string_view setting_name) const
{
    return custom_settings_prefixes->isSettingNameAllowed(setting_name);
}

void AccessControl::checkSettingNameIsAllowed(const std::string_view setting_name) const
{
    custom_settings_prefixes->checkSettingNameIsAllowed(setting_name);
}


void AccessControl::setNoPasswordAllowed(bool allow_no_password_)
{
    allow_no_password = allow_no_password_;
}

bool AccessControl::isNoPasswordAllowed() const
{
    return allow_no_password;
}

void AccessControl::setPlaintextPasswordAllowed(bool allow_plaintext_password_)
{
    allow_plaintext_password = allow_plaintext_password_;
}

bool AccessControl::isPlaintextPasswordAllowed() const
{
    return allow_plaintext_password;
}


std::shared_ptr<const ContextAccess> AccessControl::getContextAccess(
    const UUID & user_id,
    const std::vector<UUID> & current_roles,
    bool use_default_roles,
    const Settings & settings,
    const String & current_database,
    const ClientInfo & client_info) const
{
    ContextAccessParams params;
    params.user_id = user_id;
    params.current_roles.insert(current_roles.begin(), current_roles.end());
    params.use_default_roles = use_default_roles;
    params.current_database = current_database;
    params.readonly = settings.readonly;
    params.allow_ddl = settings.allow_ddl;
    params.allow_introspection = settings.allow_introspection_functions;
    params.interface = client_info.interface;
    params.http_method = client_info.http_method;
    params.address = client_info.current_address.host();
    params.quota_key = client_info.quota_key;

    /// Extract the last entry from comma separated list of X-Forwarded-For addresses.
    /// Only the last proxy can be trusted (if any).
    Strings forwarded_addresses;
    boost::split(forwarded_addresses, client_info.forwarded_for, boost::is_any_of(","));
    if (!forwarded_addresses.empty())
    {
        String & last_forwarded_address = forwarded_addresses.back();
        boost::trim(last_forwarded_address);
        params.forwarded_address = last_forwarded_address;
    }

    return getContextAccess(params);
}


std::shared_ptr<const ContextAccess> AccessControl::getContextAccess(const ContextAccessParams & params) const
{
    return context_access_cache->getContextAccess(params);
}


std::shared_ptr<const EnabledRoles> AccessControl::getEnabledRoles(
    const std::vector<UUID> & current_roles,
    const std::vector<UUID> & current_roles_with_admin_option) const
{
    return role_cache->getEnabledRoles(current_roles, current_roles_with_admin_option);
}


std::shared_ptr<const EnabledRowPolicies> AccessControl::getEnabledRowPolicies(const UUID & user_id, const boost::container::flat_set<UUID> & enabled_roles) const
{
    return row_policy_cache->getEnabledRowPolicies(user_id, enabled_roles);
}


std::shared_ptr<const EnabledRowPolicies> AccessControl::tryGetDefaultRowPolicies(const UUID & user_id) const
{
    auto user = tryRead<User>(user_id);
    if (!user)
        return nullptr;
    auto default_roles = getEnabledRoles(user->granted_roles.findGranted(user->default_roles), {})->getRolesInfo()->enabled_roles;
    return getEnabledRowPolicies(user_id, default_roles);
}


std::shared_ptr<const EnabledQuota> AccessControl::getEnabledQuota(
    const UUID & user_id,
    const String & user_name,
    const boost::container::flat_set<UUID> & enabled_roles,
    const Poco::Net::IPAddress & address,
    const String & forwarded_address,
    const String & custom_quota_key) const
{
    return quota_cache->getEnabledQuota(user_id, user_name, enabled_roles, address, forwarded_address, custom_quota_key);
}


std::vector<QuotaUsage> AccessControl::getAllQuotasUsage() const
{
    return quota_cache->getAllQuotasUsage();
}


std::shared_ptr<const EnabledSettings> AccessControl::getEnabledSettings(
    const UUID & user_id,
    const SettingsProfileElements & settings_from_user,
    const boost::container::flat_set<UUID> & enabled_roles,
    const SettingsProfileElements & settings_from_enabled_roles) const
{
    return settings_profiles_cache->getEnabledSettings(user_id, settings_from_user, enabled_roles, settings_from_enabled_roles);
}

std::shared_ptr<const SettingsProfilesInfo> AccessControl::getSettingsProfileInfo(const UUID & profile_id)
{
    return settings_profiles_cache->getSettingsProfileInfo(profile_id);
}


const ExternalAuthenticators & AccessControl::getExternalAuthenticators() const
{
    return *external_authenticators;
}

}
