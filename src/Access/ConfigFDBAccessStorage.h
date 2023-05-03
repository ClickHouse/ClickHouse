#pragma once
#include <Access/FDBAccessStorage.h>
#include <Access/MemoryAccessStorage.h>
#include <Common/FoundationDB/MetadataStoreFoundationDB.h>

namespace DB
{

/// Implementation of IAccessStorage which loads access entities from users.xml and serialize to foundationdb.
class ConfigFDBAccessStorage : public FDBAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "ConfigFDBAccessStorage";
    using CheckSettingNameFunction = std::function<void(const std::string_view &)>;
    using IsNoPasswordFunction = std::function<bool()>;
    using IsPlaintextPasswordFunction = std::function<bool()>;

    ConfigFDBAccessStorage(
        const String & storage_name_,
        const String & config_path_,
        const Poco::Util::AbstractConfiguration & config_,
        const std::function<FoundationDBPtr()> & get_fdb_function_,
        const CheckSettingNameFunction & check_setting_name_function_,
        const IsNoPasswordFunction & is_no_password_allowed_function_,
        const IsPlaintextPasswordFunction & is_plaintext_password_allowed_function_);
    ConfigFDBAccessStorage(
        const String & config_path_,
        const Poco::Util::AbstractConfiguration & config_,
        const std::function<FoundationDBPtr()> & get_fdb_function_,
        const CheckSettingNameFunction & check_setting_name_function_,
        const IsNoPasswordFunction & is_no_password_allowed_function_,
        const IsPlaintextPasswordFunction & is_plaintext_password_allowed_function_);

    const char * getStorageType() const override { return STORAGE_TYPE; }
    bool isPathEqual(const String & path_) const;
    String getPath() const;
    void setAll(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities);
    void setConfig();
    std::pair<boost::container::flat_set<UUID>, std::vector<UUID>>
    getUnusedAndConflictingEntityComparedWithMemory(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities);

private:
    CheckSettingNameFunction check_setting_name_function;
    IsNoPasswordFunction is_no_password_allowed_function;
    IsPlaintextPasswordFunction is_plaintext_password_allowed_function;
    String config_path;
    std::atomic<bool> is_first_startup;
    Poco::Util::AbstractConfiguration & config;

    std::vector<std::pair<UUID, AccessEntityPtr>> pullAllEntitiesFromFDB() const;
    void setAllNoLock(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities, Notifications & notifications);
    bool isReadOnly() const override { return true; }
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::vector<std::pair<UUID, AccessEntityPtr>> parseEntitiesFromConfig(const Poco::Util::AbstractConfiguration & config);
};

}
