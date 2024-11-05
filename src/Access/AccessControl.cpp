#include <Access/AccessControl.h>
#include <Access/MultipleAccessStorage.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/ReplicatedAccessStorage.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Access/DiskAccessStorage.h>
#include <Access/LDAPAccessStorage.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledSettings.h>
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
#include <Access/resolveSetting.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/RestorerFromBackup.h>
#include <Core/Settings.h>
#include <base/defines.h>
#include <base/range.h>
#include <IO/Operators.h>
#include <Common/re2.h>

#include <Poco/AccessExpireCache.h>
#include <boost/algorithm/string/join.hpp>
#include <filesystem>
#include <mutex>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_SETTING;
    extern const int AUTHENTICATION_FAILED;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    void checkForUsersNotInMainConfig(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_path,
        const std::string & users_config_path,
        LoggerPtr log)
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
            if ((*x)->getUserID() && !(*x)->tryGetUser())
                cache.remove(params); /// The user has been dropped while it was in the cache.
            else
                return *x;
        }

        /// TODO: There is no need to keep the `ContextAccessCache::mutex` locked while we're calculating access rights.
        auto res = std::make_shared<ContextAccess>(access_control, params);
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
        if (settingIsBuiltin(setting_name))
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
            throw Exception(ErrorCodes::UNKNOWN_SETTING,
                            "Setting {} is neither a builtin setting nor started with the prefix '{}"
                            "' registered for user-defined settings",
                            String{setting_name}, boost::algorithm::join(registered_prefixes, "' or '"));
        }

        throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown setting '{}'", String{setting_name});
    }

private:
    Strings registered_prefixes TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;
};


class AccessControl::PasswordComplexityRules
{
public:
    void setPasswordComplexityRulesFromConfig(const Poco::Util::AbstractConfiguration & config_)
    {
        std::lock_guard lock{mutex};

        rules.clear();

        if (config_.has("password_complexity"))
        {
            Poco::Util::AbstractConfiguration::Keys password_complexity;
            config_.keys("password_complexity", password_complexity);

            for (const auto & key : password_complexity)
            {
                if (key == "rule" || key.starts_with("rule["))
                {
                    String pattern(config_.getString("password_complexity." + key + ".pattern"));
                    String message(config_.getString("password_complexity." + key + ".message"));

                    auto matcher = std::make_unique<RE2>(pattern, RE2::Quiet);
                    if (!matcher->ok())
                        throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                            "Password complexity pattern {} cannot be compiled: {}",
                            pattern, matcher->error());

                    rules.push_back({std::move(matcher), std::move(pattern), std::move(message)});
                }
            }
        }
    }

    void setPasswordComplexityRules(const std::vector<std::pair<String, String>> & rules_)
    {
        Rules new_rules;

        for (const auto & [original_pattern, exception_message] : rules_)
        {
            auto matcher = std::make_unique<RE2>(original_pattern, RE2::Quiet);
            if (!matcher->ok())
                throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                    "Password complexity pattern {} cannot be compiled: {}",
                    original_pattern, matcher->error());

            new_rules.push_back({std::move(matcher), original_pattern, exception_message});
        }

        std::lock_guard lock{mutex};
        rules = std::move(new_rules);
    }

    void checkPasswordComplexityRules(const String & password_) const
    {
        String exception_text;
        bool failed = false;

        std::lock_guard lock{mutex};
        for (const auto & rule : rules)
        {
            if (!RE2::PartialMatch(password_, *rule.matcher))
            {
                failed = true;

                if (!exception_text.empty())
                    exception_text += ", ";

                exception_text += rule.exception_message;
            }
        }

        if (failed)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid password. The password should: {}", exception_text);
    }

    std::vector<std::pair<String, String>> getPasswordComplexityRules()
    {
        std::vector<std::pair<String, String>> result;

        std::lock_guard lock{mutex};
        result.reserve(rules.size());

        for (const auto & rule : rules)
            result.push_back({rule.original_pattern, rule.exception_message});

        return result;
    }

private:
    struct Rule
    {
        std::unique_ptr<RE2> matcher;
        String original_pattern;
        String exception_message;
    };

    using Rules = std::vector<Rule>;

    Rules rules TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;
};


AccessControl::AccessControl()
    : MultipleAccessStorage("user directories"),
      context_access_cache(std::make_unique<ContextAccessCache>(*this)),
      role_cache(std::make_unique<RoleCache>(*this, 600)),
      row_policy_cache(std::make_unique<RowPolicyCache>(*this)),
      quota_cache(std::make_unique<QuotaCache>(*this)),
      settings_profiles_cache(std::make_unique<SettingsProfilesCache>(*this)),
      external_authenticators(std::make_unique<ExternalAuthenticators>()),
      custom_settings_prefixes(std::make_unique<CustomSettingsPrefixes>()),
      changes_notifier(std::make_unique<AccessChangesNotifier>()),
      password_rules(std::make_unique<PasswordComplexityRules>())
{
}


AccessControl::~AccessControl()
{
    try
    {
        AccessControl::shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void AccessControl::shutdown()
{
    MultipleAccessStorage::shutdown();
    removeAllStorages();
}


void AccessControl::setupFromMainConfig(const Poco::Util::AbstractConfiguration & config_, const String & config_path_,
                                        const zkutil::GetZooKeeper & get_zookeeper_function_)
{
    if (config_.has("custom_settings_prefixes"))
        setCustomSettingsPrefixes(config_.getString("custom_settings_prefixes"));

    setImplicitNoPasswordAllowed(config_.getBool("allow_implicit_no_password", true));
    setNoPasswordAllowed(config_.getBool("allow_no_password", true));
    setPlaintextPasswordAllowed(config_.getBool("allow_plaintext_password", true));
    setDefaultPasswordTypeFromConfig(config_.getString("default_password_type", "sha256_password"));
    setPasswordComplexityRulesFromConfig(config_);

    setBcryptWorkfactor(config_.getInt("bcrypt_workfactor", 12));

    /// Optional improvements in access control system.
    /// The default values are false because we need to be compatible with earlier access configurations
    setEnabledUsersWithoutRowPoliciesCanReadRows(config_.getBool("access_control_improvements.users_without_row_policies_can_read_rows", true));
    setOnClusterQueriesRequireClusterGrant(config_.getBool("access_control_improvements.on_cluster_queries_require_cluster_grant", true));
    setSelectFromSystemDatabaseRequiresGrant(config_.getBool("access_control_improvements.select_from_system_db_requires_grant", true));
    setSelectFromInformationSchemaRequiresGrant(config_.getBool("access_control_improvements.select_from_information_schema_requires_grant", true));
    setSettingsConstraintsReplacePrevious(config_.getBool("access_control_improvements.settings_constraints_replace_previous", true));
    setTableEnginesRequireGrant(config_.getBool("access_control_improvements.table_engines_require_grant", false));

    addStoragesFromMainConfig(config_, config_path_, get_zookeeper_function_);

    role_cache = std::make_unique<RoleCache>(*this, config_.getInt("access_control_improvements.role_cache_expiration_time_seconds", 600));
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
        if ((type == "users.xml") || (type == "users_config"))
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
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown storage type '{}' at {} in config", type, prefix);
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


void AccessControl::reload(ReloadMode reload_mode)
{
    MultipleAccessStorage::reload(reload_mode);
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

bool AccessControl::insertImpl(const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id)
{
    if (MultipleAccessStorage::insertImpl(id, entity, replace_if_exists, throw_if_exists, conflicting_id))
    {
        changes_notifier->sendNotifications();
        return true;
    }
    return false;
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


AuthResult AccessControl::authenticate(const Credentials & credentials, const Poco::Net::IPAddress & address, const String & forwarded_address) const
{
    // NOTE: In the case where the user has never been logged in using LDAP,
    // Then user_id is not generated, and the authentication quota will always be nullptr.
    auto authentication_quota = getAuthenticationQuota(credentials.getUserName(), address, forwarded_address);
    if (authentication_quota)
    {
        /// Reserve a single try from the quota to check whether we have another authentication try.
        /// This is required for correct behavior in this situation:
        /// User has 1 login failures quota.
        /// * At the first login with an invalid password: Increase the quota counter. 1 (used) > 1 (max) is false.
        ///   Then try to authenticate the user and throw an AUTHENTICATION_FAILED error.
        /// * In case of the second try: increase quota counter, 2 (used) > 1 (max), then throw QUOTA_EXCEED
        ///   and don't let the user authenticate.
        ///
        /// The authentication failures counter will be reset after successful authentication.
        authentication_quota->used(QuotaType::FAILED_SEQUENTIAL_AUTHENTICATIONS, 1);
    }

    try
    {
        const auto auth_result = MultipleAccessStorage::authenticate(credentials, address, *external_authenticators, allow_no_password,
                                                                     allow_plaintext_password);
        if (authentication_quota)
            authentication_quota->reset(QuotaType::FAILED_SEQUENTIAL_AUTHENTICATIONS);

        return auth_result;
    }
    catch (...)
    {
        tryLogCurrentException(getLogger(), "from: " + address.toString() + ", user: " + credentials.getUserName()  + ": Authentication failed", LogsLevel::information);

        WriteBufferFromOwnString message;
        message << credentials.getUserName() << ": Authentication failed: password is incorrect, or there is no user with such name.";

        /// Better exception message for usability.
        /// It is typical when users install ClickHouse, type some password and instantly forget it.
        if (credentials.getUserName().empty() || credentials.getUserName() == "default")
            message << "\n\n"
                << "If you have installed ClickHouse and forgot password you can reset it in the configuration file.\n"
                << "The password for default user is typically located at /etc/clickhouse-server/users.d/default-password.xml\n"
                << "and deleting this file will reset the password.\n"
                << "See also /etc/clickhouse-server/users.xml on the server where ClickHouse is installed.\n\n";

        /// We use the same message for all authentication failures because we don't want to give away any unnecessary information for security reasons.
        /// Only the log ((*), above) will show the exact reason. Note that (*) logs at information level instead of the default error level as
        /// authentication failures are not an unusual event.
        throw Exception(PreformattedMessage{message.str(),
                                            "{}: Authentication failed: password is incorrect, or there is no user with such name",
                                            std::vector<std::string>{credentials.getUserName()}},
                        ErrorCodes::AUTHENTICATION_FAILED);
    }
}

void AccessControl::restoreFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup)
{
    MultipleAccessStorage::restoreFromBackup(restorer, data_path_in_backup);
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

void AccessControl::setImplicitNoPasswordAllowed(bool allow_implicit_no_password_)
{
    allow_implicit_no_password = allow_implicit_no_password_;
}

bool AccessControl::isImplicitNoPasswordAllowed() const
{
    return allow_implicit_no_password;
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

void AccessControl::setDefaultPasswordTypeFromConfig(const String & type_)
{
    for (auto check_type : collections::range(AuthenticationType::MAX))
    {
        const auto & info = AuthenticationTypeInfo::get(check_type);

        if (type_ == info.name && info.is_password)
        {
            default_password_type = check_type;
            return;
        }
    }

    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown password type in 'default_password_type' in config");
}

AuthenticationType AccessControl::getDefaultPasswordType() const
{
    return default_password_type;
}

void AccessControl::setPasswordComplexityRulesFromConfig(const Poco::Util::AbstractConfiguration & config_)
{
    password_rules->setPasswordComplexityRulesFromConfig(config_);
}

void AccessControl::setPasswordComplexityRules(const std::vector<std::pair<String, String>> & rules_)
{
    password_rules->setPasswordComplexityRules(rules_);
}

void AccessControl::checkPasswordComplexityRules(const String & password_) const
{
    password_rules->checkPasswordComplexityRules(password_);
}

std::vector<std::pair<String, String>> AccessControl::getPasswordComplexityRules() const
{
    return password_rules->getPasswordComplexityRules();
}

void AccessControl::setBcryptWorkfactor(int workfactor_)
{
    if (workfactor_ < 4)
        bcrypt_workfactor = 4;
    else if (workfactor_ > 31)
        bcrypt_workfactor = 31;
    else
        bcrypt_workfactor = workfactor_;
}

int AccessControl::getBcryptWorkfactor() const
{
    return bcrypt_workfactor;
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


std::shared_ptr<const EnabledRolesInfo> AccessControl::getEnabledRolesInfo(
    const std::vector<UUID> & current_roles,
    const std::vector<UUID> & current_roles_with_admin_option) const
{
    return getEnabledRoles(current_roles, current_roles_with_admin_option)->getRolesInfo();
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
    return quota_cache->getEnabledQuota(user_id, user_name, enabled_roles, address, forwarded_address, custom_quota_key, true);
}

std::shared_ptr<const EnabledQuota> AccessControl::getAuthenticationQuota(
    const String & user_name, const Poco::Net::IPAddress & address, const std::string & forwarded_address) const
{
    auto user_id = find<User>(user_name);
    UserPtr user;
    if (user_id && (user = tryRead<User>(*user_id)))
    {
        const auto new_current_roles = user->granted_roles.findGranted(user->default_roles);
        const auto roles_info = getEnabledRolesInfo(new_current_roles, {});

        // client_key is not received at the moment of authentication during TCP connection
        // if key type is set to QuotaKeyType::CLIENT_KEY
        // QuotaCache::QuotaInfo::calculateKey will throw exception without throw_if_client_key_empty = false
        String quota_key;
        bool throw_if_client_key_empty = false;
        return quota_cache->getEnabledQuota(*user_id,
                                            user->getName(),
                                            roles_info->enabled_roles,
                                            address,
                                            forwarded_address,
                                            quota_key,
                                            throw_if_client_key_empty);
    }
    return nullptr;
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

std::shared_ptr<const SettingsProfilesInfo> AccessControl::getEnabledSettingsInfo(
    const UUID & user_id,
    const SettingsProfileElements & settings_from_user,
    const boost::container::flat_set<UUID> & enabled_roles,
    const SettingsProfileElements & settings_from_enabled_roles) const
{
    return getEnabledSettings(user_id, settings_from_user, enabled_roles, settings_from_enabled_roles)->getInfo();
}

std::shared_ptr<const SettingsProfilesInfo> AccessControl::getSettingsProfileInfo(const UUID & profile_id)
{
    return settings_profiles_cache->getSettingsProfileInfo(profile_id);
}


const ExternalAuthenticators & AccessControl::getExternalAuthenticators() const
{
    return *external_authenticators;
}


void AccessControl::allowAllSettings()
{
    custom_settings_prefixes->registerPrefixes({""});
}

}
