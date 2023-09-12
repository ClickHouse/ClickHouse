#pragma once

#include <unordered_map>
#include <Access/IAccessStorage.h>
#include <Access/AccessChangesNotifier.h>
#include <boost/container/flat_set.hpp>
#include <Common/FoundationDB/MetadataStoreFoundationDB.h>

namespace DB
{

class FDBAccessStorage : public IAccessStorage
{
protected:
    using AccessEntityScope = MetadataStoreFoundationDB::AccessEntityScope;
    using WriteEntityInFDBFunc = std::function<void(const UUID & /* id */, const IAccessEntity & /* new or changed entity */)>;
    using DeleteEntityInFDBFunc = std::function<void(const UUID & /* id */, const AccessEntityType & /* type */)>;
    using FoundationDBPtr = std::shared_ptr<MetadataStoreFoundationDB>;

    FDBAccessStorage(const String & storage_name_, AccessChangesNotifier & changes_notifier_, std::shared_ptr<MetadataStoreFoundationDB> meta_store_);

    struct Entry
    {
        UUID id;
        String name;
        AccessEntityType type;
        mutable AccessEntityPtr entity; /// may be nullptr in SqlDrivenFDBAccessStorage, if the entity hasn't been loaded yet.
    };

    bool isReadOnly() const override { return readonly; }

    std::shared_ptr<MetadataStoreFoundationDB> meta_store;
    AccessEntityScope scope;

    std::atomic<bool> readonly;
    mutable std::mutex mutex;
    std::unordered_map<UUID, Entry> entries_by_id;
    std::unordered_map<std::string_view, Entry *> entries_by_name_and_type[static_cast<size_t>(AccessEntityType::MAX)];

    AccessChangesNotifier & changes_notifier;


    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;


    bool removeNoLock(const UUID & id, bool throw_if_not_exists, const DeleteEntityInFDBFunc & delete_func = {});
    bool updateNoLock(const UUID & id, const UpdateFunc & update_func,bool throw_if_not_exists,const WriteEntityInFDBFunc & write_func = {});
    bool insertNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists, const WriteEntityInFDBFunc & write_func = {});


    void tryInsertEntityToFDB(const UUID & id, const IAccessEntity & entity) const;
    AccessEntityPtr tryReadEntityFromFDB(const UUID & id) const;
    void tryDeleteEntityOnFDB(const UUID & id, const AccessEntityType & type) const;
    void tryUpdateEntityOnFDB(const UUID & id, const IAccessEntity & entity) const;
    void tryClearEntitiesOnFDB() const;

    bool exists(const UUID & id) const override;
};

}
