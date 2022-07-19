#pragma once

#include <Access/MemoryAccessStorage.h>
#include <Common/ZooKeeper/Common.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB
{
class ConfigReloader;

/// Implementation of IAccessStorage which loads all from users.xml periodically.
class UsersConfigAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "users.xml";
    using CheckSettingNameFunction = std::function<void(const std::string_view &)>;

    UsersConfigAccessStorage(const String & storage_name_ = STORAGE_TYPE, const CheckSettingNameFunction & check_setting_name_function_ = {});
    UsersConfigAccessStorage(const CheckSettingNameFunction & check_setting_name_function_);
    ~UsersConfigAccessStorage() override;

    const char * getStorageType() const override { return STORAGE_TYPE; }
    String getStorageParamsJSON() const override;

    String getPath() const;
    bool isPathEqual(const String & path_) const;

    void setConfig(const Poco::Util::AbstractConfiguration & config);

    void load(const String & users_config_path,
              const String & include_from_path = {},
              const String & preprocessed_dir = {},
              const zkutil::GetZooKeeper & get_zookeeper_function = {});
    void reload();
    void startPeriodicReloading();

private:
    void parseFromConfig(const Poco::Util::AbstractConfiguration & config);

    std::optional<UUID> findImpl(EntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(EntityType type) const override;
    bool existsImpl(const UUID & id) const override;
    AccessEntityPtr readImpl(const UUID & id) const override;
    String readNameImpl(const UUID & id) const override;
    bool canInsertImpl(const AccessEntityPtr &) const override { return false; }
    UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    scope_guard subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const override;
    bool hasSubscriptionImpl(const UUID & id) const override;
    bool hasSubscriptionImpl(EntityType type) const override;

    MemoryAccessStorage memory_storage;
    CheckSettingNameFunction check_setting_name_function;

    String path;
    std::unique_ptr<ConfigReloader> config_reloader;
    mutable std::mutex load_mutex;
};
}
