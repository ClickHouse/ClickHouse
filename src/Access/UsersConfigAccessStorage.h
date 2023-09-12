#pragma once

#include <Access/Quota.h>
#include <Access/RowPolicy.h>
#include <Access/User.h>
#include <Access/SettingsProfile.h>
#include <Access/MemoryAccessStorage.h>
#include <Common/ZooKeeper/Common.h>
#include <base/FnTraits.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB
{
class AccessControl;
class ConfigReloader;

/// Implementation of IAccessStorage which loads all from users.xml periodically.
class UsersConfigAccessStorage : public IAccessStorage
{
public:

    static constexpr char STORAGE_TYPE[] = "users.xml";

    UsersConfigAccessStorage(const String & storage_name_, AccessControl & access_control_, bool allow_backup_);
    ~UsersConfigAccessStorage() override;

    const char * getStorageType() const override { return STORAGE_TYPE; }
    String getStorageParamsJSON() const override;
    bool isReadOnly() const override { return true; }

    String getPath() const;
    bool isPathEqual(const String & path_) const;

    void setConfig(const Poco::Util::AbstractConfiguration & config);
    void load(const String & users_config_path,
              const String & include_from_path = {},
              const String & preprocessed_dir = {},
              const zkutil::GetZooKeeper & get_zookeeper_function = {});

    void startPeriodicReloading() override;
    void stopPeriodicReloading() override;
    void reload(ReloadMode reload_mode) override;

    bool exists(const UUID & id) const override;

    bool isBackupAllowed() const override { return backup_allowed; }

private:
    void parseFromConfig(const Poco::Util::AbstractConfiguration & config);
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<std::pair<String, AccessEntityType>> readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const override;

    AccessControl & access_control;
    MemoryAccessStorage memory_storage;
    String path;
    std::unique_ptr<ConfigReloader> config_reloader;
    bool backup_allowed = false;
    mutable std::mutex load_mutex;

public:
    static UUID generateID(AccessEntityType type, const String & name);
    static UUID generateID(const IAccessEntity & entity);
    static UserPtr parseUser(const Poco::Util::AbstractConfiguration & config, const String & user_name, const std::unordered_set<UUID> & allowed_profile_ids, bool allow_no_password, bool allow_plaintext_password);
    static std::vector<AccessEntityPtr> parseUsers(const Poco::Util::AbstractConfiguration & config, const std::unordered_set<UUID> & allowed_profile_ids, bool allow_no_password, bool allow_plaintext_password);
    static QuotaPtr
    parseQuota(const Poco::Util::AbstractConfiguration & config, const String & quota_name, const std::vector<UUID> & user_ids);
    static std::vector<AccessEntityPtr> parseQuotas(const Poco::Util::AbstractConfiguration & config);
    static std::vector<AccessEntityPtr> parseRowPolicies(const Poco::Util::AbstractConfiguration & config, bool users_without_row_policies_can_read_rows);
    static SettingsProfileElements parseSettingsConstraints(const Poco::Util::AbstractConfiguration & config, const String & path_to_constraints, const AccessControl & access_control);
    static std::shared_ptr<SettingsProfile> parseSettingsProfile(const Poco::Util::AbstractConfiguration & config, const String & profile_name, const std::unordered_set<UUID> & allowed_parent_profile_ids, const AccessControl & access_control);
    static std::vector<AccessEntityPtr> parseSettingsProfiles(const Poco::Util::AbstractConfiguration & config, const std::unordered_set<UUID> & allowed_parent_profile_ids, const AccessControl & access_control);
    static std::unordered_set<UUID> getAllowedSettingsProfileIDs(const Poco::Util::AbstractConfiguration & config);
};
}
