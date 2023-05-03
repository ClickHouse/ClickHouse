#pragma once

#include <Access/FDBAccessStorage.h>
#include <boost/container/flat_set.hpp>
#include <Common/FoundationDB/MetadataStoreFoundationDB.h>
#include <Common/ThreadPool.h>


namespace DB
{
/// Loads all access entities on the local disk when node starts firstly and implements the sql-driven way of acl management in foundationdb.
class SqlDrivenFDBAccessStorage : public FDBAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "SqlDrivenFDBAccessStorage";

    SqlDrivenFDBAccessStorage(
        const String & storage_name_,
        const String & local_directory_path_,
        const std::function<FoundationDBPtr()> & get_fdb_function_,
        bool readonly_ = false);
    SqlDrivenFDBAccessStorage(
        const String & directory_path_, const std::function<FoundationDBPtr()> & get_fdb_function_, bool readonly_ = false);
    ~SqlDrivenFDBAccessStorage() override;

    const char * getStorageType() const override { return STORAGE_TYPE; }
    String getPath() const { return local_directory_path; }
    bool isPathEqual(const String & local_directory_path_) const;
    void setReadOnly(bool readonly_) { readonly = readonly_; }

private:
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<String> readNameImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<UUID> insertImpl(const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists) override;
    bool removeImpl(const UUID & id, bool throw_if_not_exists) override;
    bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) override;

    void clear();
    void readLists();
    void rebuildListsFromDisk();

    void migrateMetadataFromLocalToFDB();
    std::vector<std::pair<UUID, std::string>> tryReadListsByTypeFromFDB(const AccessEntityType & type) const;
    String local_directory_path;
};
}
