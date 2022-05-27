#pragma once

#include <Access/MemoryAccessStorage.h>
#include <Common/ZooKeeper/Common.h>


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

    UsersConfigAccessStorage(const String & storage_name_, AccessControl & access_control_);
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

    void reload() override;
    void startPeriodicReloading() override;
    void stopPeriodicReloading() override;

    bool exists(const UUID & id) const override;

private:
    void parseFromConfig(const Poco::Util::AbstractConfiguration & config);
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<String> readNameImpl(const UUID & id, bool throw_if_not_exists) const override;

    AccessControl & access_control;
    MemoryAccessStorage memory_storage;
    String path;
    std::unique_ptr<ConfigReloader> config_reloader;
    mutable std::mutex load_mutex;
};
}
