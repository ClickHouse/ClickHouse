#pragma once
#include <Access/AccessControl.h>
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

    ConfigFDBAccessStorage(
        const String & storage_name_,
        AccessChangesNotifier & changes_notifier_,
        AccessControl & access_control_,
        const Poco::Util::AbstractConfiguration & config_,
        const String &  config_path_,
        const std::function<FoundationDBPtr()> & get_fdb_function_);
    ConfigFDBAccessStorage(
        AccessChangesNotifier & changes_notifier_,
        AccessControl & access_control_,
        const Poco::Util::AbstractConfiguration & config_,
        const String & config_path_,
        const std::function<FoundationDBPtr()> & get_fdb_function_);

    const char * getStorageType() const override { return STORAGE_TYPE; }
    bool isPathEqual(const String & path_) const;
    String getPath() const;
    void setAll(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities);
    void setConfig();
    std::pair<boost::container::flat_set<UUID>, std::vector<UUID>>
    getUnusedAndConflictingEntityComparedWithMemory(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities);

private:
    String config_path;
    Poco::Util::AbstractConfiguration & config;
    std::atomic<bool> is_first_startup;
    AccessControl & access_control;

    std::vector<std::pair<UUID, AccessEntityPtr>> pullAllEntitiesFromFDB() const;
    void setAllNoLock(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities);
    bool isReadOnly() const override { return true; }
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::vector<std::pair<UUID, AccessEntityPtr>> parseEntitiesFromConfig(const Poco::Util::AbstractConfiguration & config_);
};

}
