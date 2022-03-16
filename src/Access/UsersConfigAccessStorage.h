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
    using IsNoPasswordFunction = std::function<bool()>;
    using IsPlaintextPasswordFunction =  std::function<bool()>;

    UsersConfigAccessStorage(const String & storage_name_ = STORAGE_TYPE, const CheckSettingNameFunction & check_setting_name_function_ = {}, const IsNoPasswordFunction & is_no_password_allowed_function_ ={}, const IsPlaintextPasswordFunction & is_plaintext_password_allowed_function_ = {}); /// NOLINT
    UsersConfigAccessStorage(const CheckSettingNameFunction & check_setting_name_function_, const IsNoPasswordFunction & is_no_password_allowed_function_, const IsPlaintextPasswordFunction & is_plaintext_password_allowed_function_); /// NOLINT

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
    void reload();
    void startPeriodicReloading();
    void stopPeriodicReloading();

    bool exists(const UUID & id) const override;
    bool hasSubscription(const UUID & id) const override;
    bool hasSubscription(AccessEntityType type) const override;

private:
    void parseFromConfig(const Poco::Util::AbstractConfiguration & config);
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<String> readNameImpl(const UUID & id, bool throw_if_not_exists) const override;
    scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    scope_guard subscribeForChangesImpl(AccessEntityType type, const OnChangedHandler & handler) const override;

    MemoryAccessStorage memory_storage;
    CheckSettingNameFunction check_setting_name_function;
    IsNoPasswordFunction is_no_password_allowed_function;
    IsPlaintextPasswordFunction is_plaintext_password_allowed_function;
    String path;
    std::unique_ptr<ConfigReloader> config_reloader;
    mutable std::mutex load_mutex;
};
}
