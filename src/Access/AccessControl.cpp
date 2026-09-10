#include <Access/AccessBackup.h>
#include <Access/AccessChangesNotifier.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/DiskAccessStorage.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/EnabledSettings.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/LDAPAccessStorage.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/MultipleAccessStorage.h>
#include <Access/QuotaCache.h>
#include <Access/QuotaUsage.h>
#include <Access/ReplicatedAccessStorage.h>
#include <Access/RoleCache.h>
#include <Access/RowPolicyCache.h>
#include <Access/SettingsConstraints.h>
#include <Access/SettingsProfilesCache.h>
#include <Access/User.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Access/resolveEffectiveSettings.h>
#include <Access/resolveSetting.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/IRestoreCoordination.h>
#include <Backups/RestorerFromBackup.h>
#include <Core/Settings.h>
#include <IO/Operators.h>
#include <base/range.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
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
    extern const int REQUIRED_SECOND_FACTOR;
    extern const int REQUIRED_PASSWORD;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int BAD_ARGUMENTS;
}

namespace FailPoints
{
extern const char access_control_pause_after_feature_tier_check[];
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
        if (auto cached_access = cache.get(params))
        {
            /// Check that the user hasn't been dropped while it was in the cache.
            if (!(*cached_access)->getUserID() || (*cached_access)->tryGetUser())
                return *cached_access;
        }

        auto res = std::make_shared<ContextAccess>(access_control, params);
        res->initialize();
        cache.add(params, res);
        return res;
    }

private:
    const AccessControl & access_control;
    Poco::AccessExpireCache<ContextAccess::Params, std::shared_ptr<const ContextAccess>> cache;
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


class AccessControl::RestoreAccessStorage : public IAccessStorage
{
public:
    RestoreAccessStorage(AccessControl & access_control_, IAccessStorage & destination_)
        : IAccessStorage(destination_.getStorageName())
        , access_control(access_control_)
        , destination(destination_)
    {
    }

    const char * getStorageType() const override { return destination.getStorageType(); }
    bool isReadOnly() const override { return destination.isReadOnly(); }
    bool isReadOnly(const UUID & id) const override { return access_control.isReadOnly(id); }
    bool exists(const UUID & id) const override { return access_control.exists(id); }

protected:
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override { return access_control.find(type, name); }

    std::vector<UUID> findAllImpl(AccessEntityType type) const override { return access_control.findAll(type); }

    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override
    {
        return access_control.read(id, throw_if_not_exists);
    }

    bool insertImpl(
        const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id) override
    {
        return access_control.insertImpl(&destination, id, entity, replace_if_exists, throw_if_exists, conflicting_id);
    }

    bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) override
    {
        return access_control.update(id, update_func, throw_if_not_exists);
    }

private:
    AccessControl & access_control;
    IAccessStorage & destination;
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
    setEnabledUsersWithoutRowPoliciesCanReadRows(config_.getBool("access_control_improvements.users_without_row_policies_can_read_rows", true));
    setOnClusterQueriesRequireClusterGrant(config_.getBool("access_control_improvements.on_cluster_queries_require_cluster_grant", true));
    setSelectFromSystemDatabaseRequiresGrant(config_.getBool("access_control_improvements.select_from_system_db_requires_grant", true));

    /// Keep in sync with `attachSystemTables`: `system.user_query_log` is attached (and thus safe to grant
    /// SELECT on implicitly) only when this is enabled. When disabled, the name is free for a regular table.
    setUserQueryLogEnabled(config_.getBool("query_log.enable_user_query_log", true));

    setSelectFromInformationSchemaRequiresGrant(config_.getBool("access_control_improvements.select_from_information_schema_requires_grant", true));
    setSettingsConstraintsReplacePrevious(config_.getBool("access_control_improvements.settings_constraints_replace_previous", true));
    setImpersonateUserAllowed(config_.getBool("access_control_improvements.allow_impersonate_user", config_.getBool("allow_impersonate_user", true)));

    /// The default values of the following improvements are false because we need to be compatible with earlier access configurations
    setTableEnginesRequireGrant(config_.getBool("access_control_improvements.table_engines_require_grant", false));
    setEnableReadWriteGrants(config_.getBool("access_control_improvements.enable_read_write_grants", false));
    setThrowOnUnmatchedRowPolicies(config_.getBool("access_control_improvements.throw_on_unmatched_row_policies", false));

    /// Set `true` by default because the feature is backward incompatible only when older version replicas are in the same cluster.
    setEnableUserNameAccessType(config_.getBool("access_control_improvements.enable_user_name_access_type", true));

    setThrowOnInvalidReplicatedAccessEntities(config_.getBool("access_control_improvements.throw_on_invalid_replicated_access_entities", false));

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
    auto new_storage = std::make_shared<ReplicatedAccessStorage>(
        storage_name_,
        zookeeper_path_,
        get_zookeeper_function_,
        *changes_notifier,
        allow_backup_,
        throw_on_invalid_replicated_access_entities);
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
    String disk_storage_dir = config.getString("access_control_path", "");
    if (!disk_storage_dir.empty())
        addDiskStorage(DiskAccessStorage::STORAGE_TYPE, disk_storage_dir, /* readonly= */ false, /* allow_backup= */ true);

    String config_dir = std::filesystem::path{config_path}.remove_filename().string();
    String dbms_dir = config.getString("path", DBMS_DEFAULT_PATH);
    String include_from_path = config.getString("include_from", "");
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
    return insertImpl(nullptr, id, entity, replace_if_exists, throw_if_exists, conflicting_id);
}

bool AccessControl::insertImpl(
    IAccessStorage * storage,
    const UUID & id,
    const AccessEntityPtr & entity,
    bool replace_if_exists,
    bool throw_if_exists,
    UUID * conflicting_id)
{
    bool inserted;
    {
        std::lock_guard lock{access_entities_mutex};
        inserted = insertImplUnlocked(storage, id, entity, replace_if_exists, throw_if_exists, conflicting_id);
    }
    if (inserted)
        changes_notifier->sendNotifications();
    return inserted;
}

void AccessControl::checkNameCollisionInOtherStorage(IAccessStorage & storage, const AccessEntityPtr & entity) const
{
    for (const auto & other_storage : getStorages())
    {
        if (other_storage.get() == &storage)
            continue;
        if (other_storage->find(entity->getType(), entity->getName()))
            throwNameCollisionCannotInsert(entity->getType(), entity->getName(), other_storage->getStorageName());
    }
}

bool AccessControl::insertImplUnlocked(
    IAccessStorage * storage,
    const UUID & id,
    const AccessEntityPtr & entity,
    bool replace_if_exists,
    bool throw_if_exists,
    UUID * conflicting_id)
{
    if (storage)
        checkNameCollisionInOtherStorage(*storage, entity);

    StoragePtr selected_storage;
    if (!storage)
        selected_storage = getStorageForInsertion(entity);
    auto & storage_for_insertion = storage ? *storage : *selected_storage;
    auto existing_id = storage_for_insertion.find(entity->getType(), entity->getName());

    /// Preserve the storage's collision behavior and, in particular, do not validate a no-op
    /// `CREATE ... IF NOT EXISTS` as if it inserted a new entity.
    if (existing_id && !replace_if_exists)
    {
        if (storage)
            return storage->insert(id, entity, replace_if_exists, throw_if_exists, conflicting_id);
        return MultipleAccessStorage::insertImpl(id, entity, replace_if_exists, throw_if_exists, conflicting_id);
    }

    if (isAnyFeatureTierRestricted(*this))
    {
        PendingAccessEntities pending;
        pending[id] = entity;
        /// A replacement drops the entity that holds the name, which need not be `id`.
        if (replace_if_exists && existing_id && *existing_id != id)
            pending[*existing_id] = nullptr;
        checkFeatureTierForPendingAccessEntities(*this, pending);
        FailPointInjection::pauseFailPoint(FailPoints::access_control_pause_after_feature_tier_check);
    }

    return storage ? storage->insert(id, entity, replace_if_exists, throw_if_exists, conflicting_id)
                   : MultipleAccessStorage::insertImpl(id, entity, replace_if_exists, throw_if_exists, conflicting_id);
}

bool AccessControl::removeImpl(const UUID & id, bool throw_if_not_exists)
{
    {
        std::lock_guard lock{access_entities_mutex};
        if (isAnyFeatureTierRestricted(*this) && exists(id))
        {
            checkFeatureTierForPendingAccessEntities(*this, PendingAccessEntities{{id, nullptr}});
            FailPointInjection::pauseFailPoint(FailPoints::access_control_pause_after_feature_tier_check);
        }

        if (!MultipleAccessStorage::removeImpl(id, throw_if_not_exists))
            return false;
    }

    changes_notifier->sendNotifications();
    return true;
}

bool AccessControl::updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    bool updated;
    {
        std::lock_guard lock{access_entities_mutex};
        const bool check_feature_tier = isAnyFeatureTierRestricted(*this);
        auto storage = findStorage(id);
        const bool pause_in_update_func = check_feature_tier && storage && storage->isReplicated();
        FeatureTierAccessEntityChecker feature_tier_checker;
        if (check_feature_tier)
        {
            auto old_entity = tryRead(id);
            if (old_entity)
            {
                auto new_entity = update_func(old_entity, id);
                feature_tier_checker = prepareFeatureTierAccessEntityChecker(
                    *this, PendingAccessEntities{{id, new_entity}}, PendingAccessEntities{{id, old_entity}});
            }
            else
            {
                /// A replicated storage can know about an entity before its local cache catches up.
                feature_tier_checker
                    = prepareFeatureTierAccessEntityChecker(*this, /* pending= */ {}, /* current= */ {}, /* force= */ true);
            }
        }

        /// A regular storage calls the update function while holding its own mutex. Pause before entering it
        /// so that authentication can still read the storage and notify this test-only failpoint. A replicated
        /// storage needs the pause inside the callback to exercise its compare-and-set retry.
        if (check_feature_tier && !pause_in_update_func)
            FailPointInjection::pauseFailPoint(FailPoints::access_control_pause_after_feature_tier_check);

        auto validating_update_func = [&](const AccessEntityPtr & old_entity, const UUID & current_id)
        {
            auto new_entity = update_func(old_entity, current_id);
            if (feature_tier_checker)
                feature_tier_checker(PendingAccessEntities{{current_id, new_entity}}, PendingAccessEntities{{current_id, old_entity}});
            if (pause_in_update_func)
                FailPointInjection::pauseFailPoint(FailPoints::access_control_pause_after_feature_tier_check);
            return new_entity;
        };

        updated = MultipleAccessStorage::updateImpl(id, validating_update_func, throw_if_not_exists);
    }
    if (updated)
        changes_notifier->sendNotifications();
    return updated;
}

std::vector<UUID> AccessControl::insertInto(
    const String & storage_name, const std::vector<AccessEntityPtr> & entities, bool replace_if_exists, bool throw_if_exists)
{
    auto storage = getStorageByName(storage_name);
    std::vector<UUID> inserted_ids;
    inserted_ids.reserve(entities.size());

    try
    {
        std::lock_guard lock{access_entities_mutex};

        /// Check all cross-storage collisions before inserting anything. An exception must not leave
        /// a prefix of a multi-entity `CREATE` statement in the destination storage.
        for (const auto & entity : entities)
            checkNameCollisionInOtherStorage(*storage, entity);

        for (const auto & entity : entities)
        {
            auto id = generateRandomID();
            if (insertImplUnlocked(storage.get(), id, entity, replace_if_exists, throw_if_exists, nullptr))
                inserted_ids.push_back(id);
        }
    }
    catch (...)
    {
        if (!inserted_ids.empty())
            changes_notifier->sendNotifications();
        throw;
    }

    if (!inserted_ids.empty())
        changes_notifier->sendNotifications();
    return inserted_ids;
}

void AccessControl::moveAccessEntities(
    const std::vector<UUID> & ids, const String & source_storage_name, const String & destination_storage_name)
{
    try
    {
        std::lock_guard lock{access_entities_mutex};
        MultipleAccessStorage::moveAccessEntities(ids, source_storage_name, destination_storage_name);
    }
    catch (...)
    {
        changes_notifier->sendNotifications();
        throw;
    }
    changes_notifier->sendNotifications();
}

AccessChangesNotifier & AccessControl::getChangesNotifier()
{
    return *changes_notifier;
}


AuthResult AccessControl::authenticate(const Credentials & credentials, const Poco::Net::IPAddress & address, const ClientInfo & client_info) const
{
    // NOTE: In the case where the user has never been logged in using LDAP,
    // Then user_id is not generated, and the authentication quota will always be nullptr.
    auto authentication_quota = getAuthenticationQuota(credentials.getUserName(), address, client_info.getLastForwardedForHost());
    if (authentication_quota)
    {
        /// Reserve a single try from the quota to check whether we have another authentication try.
        /// This is required for correct behavior in this situation:
        /// User has 1 login failures quota.
        /// * At the first login with an invalid password: Increase the quota counter. 1 (used) > 1 (max) is false.
        ///   Then try to authenticate the user and throw an AUTHENTICATION_FAILED error.
        /// * In case of the second try: increase quota counter, 2 (used) > 1 (max), then throw QUOTA_EXCEED
        ///   and don't let the user authenticate.
        ///
        /// The authentication failures counter will be reset after successful authentication.
        authentication_quota->used(QuotaType::FAILED_SEQUENTIAL_AUTHENTICATIONS, 1);
    }

    try
    {
        const auto auth_result = MultipleAccessStorage::authenticate(credentials, address, *external_authenticators, client_info,
                                                                     allow_no_password, allow_plaintext_password);
        if (authentication_quota)
            authentication_quota->reset(QuotaType::FAILED_SEQUENTIAL_AUTHENTICATIONS);

        return auth_result;
    }
    catch (...)
    {
        tryLogCurrentException(getLogger(), "from: " + address.toString() + ", user: " + credentials.getUserName()  + ": Authentication failed", LogsLevel::information);

        int error_code = ErrorCodes::AUTHENTICATION_FAILED;

        WriteBufferFromOwnString message;
        message << credentials.getUserName() << ": Authentication failed: password is incorrect, or there is no user with such name";

        /// Better exception message for usability.
        /// It is typical when users install ClickHouse, type some password and instantly forget it.
        if (credentials.getUserName().empty() || credentials.getUserName() == "default")
        {
            if (credentials.allowInteractiveBasicAuthenticationInTheBrowser())
                error_code = ErrorCodes::REQUIRED_PASSWORD;

            message << R"(

If you use ClickHouse Cloud, the password can be reset at https://clickhouse.cloud/
on the settings page for the corresponding service.

If you have installed ClickHouse and forgot password you can reset it in the configuration file.
The password for default user is typically located at /etc/clickhouse-server/users.d/default-password.xml
and deleting this file will reset the password.
See also /etc/clickhouse-server/users.xml on the server where ClickHouse is installed.

)";
        }

        /// Preserve the second factor authentication error code.
        if (getCurrentExceptionCode() == ErrorCodes::REQUIRED_SECOND_FACTOR)
            error_code = ErrorCodes::REQUIRED_SECOND_FACTOR;

        /// We use the same message for all authentication failures because we don't want to give away any unnecessary information for security reasons.
        /// Only the log ((*), above) will show the exact reason. Note that (*) logs at information level instead of the default error level as
        /// authentication failures are not an unusual event.
        throw Exception(PreformattedMessage{message.str(),
            "{}: Authentication failed: password is incorrect, or there is no user with such name",
            std::vector<std::string>{credentials.getUserName()}}, error_code);
    }
}

void AccessControl::restoreFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup)
{
    StoragePtr destination;
    for (const auto & storage : getStorages())
    {
        if (storage->isRestoreAllowed())
        {
            destination = storage;
            break;
        }
    }

    if (!destination)
        throwRestoreNotAllowed();

    if (destination->isReplicated())
    {
        auto restore_coordination = restorer.getRestoreCoordination();
        if (!restore_coordination->acquireReplicatedAccessStorage(destination->getReplicationID()))
            return;
    }

    restorer.addDataRestoreTask(
        [this, destination, &restorer, data_path_in_backup]
        {
            auto entities_to_restore = restorer.getAccessEntitiesToRestore(data_path_in_backup);
            const auto & restore_settings = restorer.getRestoreSettings();
            RestoreAccessStorage restore_storage(*this, *destination);
            restoreAccessEntitiesFromBackup(restore_storage, entities_to_restore, restore_settings);
        });
}

void AccessControl::setExternalAuthenticatorsConfig(const Poco::Util::AbstractConfiguration & config)
{
    external_authenticators->setConfiguration(config, getLogger());
}


void AccessControl::setDefaultProfileName(const String & default_profile_name)
{
    settings_profiles_cache->setDefaultProfileName(default_profile_name);
}

std::optional<UUID> AccessControl::getDefaultProfileId() const
{
    return settings_profiles_cache->getDefaultProfileId();
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

void AccessControl::setEnableUserNameAccessType(bool enable_user_name_access_type_)
{
    enable_user_name_access_type = enable_user_name_access_type_;
}

bool AccessControl::isEnabledUserNameAccessType() const
{
    return enable_user_name_access_type;
}

void AccessControl::setEnableReadWriteGrants(bool enable_read_write_grants_)
{
    enable_read_write_grants = enable_read_write_grants_;
}

bool AccessControl::isEnabledReadWriteGrants() const
{
    return enable_read_write_grants;
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
    const std::shared_ptr<Poco::Net::IPAddress> & address,
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
                                            std::make_shared<Poco::Net::IPAddress>(address),
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

void AccessControl::setAllowTierSettings(UInt32 value)
{
    allow_experimental_tier_settings = value == 0;
    allow_private_preview_tier_settings = value <= 1;
    allow_beta_tier_settings = value <= 2;
}

UInt32 AccessControl::getAllowTierSettings() const
{
    if (allow_experimental_tier_settings)
        return 0;
    if (allow_private_preview_tier_settings)
        return 1;
    if (allow_beta_tier_settings)
        return 2;
    return 3;
}

bool AccessControl::getAllowExperimentalTierSettings() const
{
    return allow_experimental_tier_settings;
}

bool AccessControl::getAllowPrivatePreviewTierSettings() const
{
    return allow_private_preview_tier_settings;
}

bool AccessControl::getAllowBetaTierSettings() const
{
    return allow_beta_tier_settings;
}
}
