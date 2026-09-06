#include <Core/Settings.h>
#include <base/sleep.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <Common/NamedCollections/NamedCollectionConfiguration.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/NamedCollections/NamedCollectionsMetadataStorage.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>

namespace CurrentMetrics
{
    extern const Metric NamedCollection;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NAMED_COLLECTION_DOESNT_EXIST;
    extern const int NAMED_COLLECTION_ALREADY_EXISTS;
    extern const int NAMED_COLLECTION_IS_IMMUTABLE;
    extern const int LOGICAL_ERROR;
}


NamedCollectionFactory & NamedCollectionFactory::instance()
{
    static NamedCollectionFactory instance;
    return instance;
}

NamedCollectionFactory::~NamedCollectionFactory()
{
    shutdown();
}

void NamedCollectionFactory::shutdown()
{
    shutdown_called = true;
    if (update_task)
        update_task->deactivate();
    metadata_storage.reset();
}

bool NamedCollectionFactory::exists(const std::string & collection_name) const
{
    std::lock_guard lock(mutex);
    return exists(collection_name, lock);
}

NamedCollectionPtr NamedCollectionFactory::get(const std::string & collection_name) const
{
    std::lock_guard lock(mutex);
    auto collection = tryGet(collection_name, lock);
    if (!collection)
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "There is no named collection `{}`",
            collection_name);
    }
    return collection;
}

NamedCollectionPtr NamedCollectionFactory::tryGet(const std::string & collection_name) const
{
    std::lock_guard lock(mutex);
    return tryGet(collection_name, lock);
}

NamedCollectionsMap NamedCollectionFactory::getAll() const
{
    std::lock_guard lock(mutex);
    return loaded_named_collections;
}

bool NamedCollectionFactory::exists(const std::string & collection_name, std::lock_guard<std::mutex> &) const
{
    return loaded_named_collections.contains(collection_name);
}

MutableNamedCollectionPtr NamedCollectionFactory::tryGet(
    const std::string & collection_name,
    std::lock_guard<std::mutex> &) const
{
    auto it = loaded_named_collections.find(collection_name);
    if (it == loaded_named_collections.end())
        return nullptr;
    return it->second;
}

MutableNamedCollectionPtr NamedCollectionFactory::getMutable(
    const std::string & collection_name,
    std::lock_guard<std::mutex> & lock) const
{
    auto collection = tryGet(collection_name, lock);
    if (!collection)
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "There is no named collection `{}`",
            collection_name);
    }
    if (!collection->isMutable())
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_IS_IMMUTABLE,
            "Cannot get collection `{}` for modification, "
            "because collection was defined as immutable",
            collection_name);
    }
    return collection;
}

void NamedCollectionFactory::add(
    const std::string & collection_name,
    MutableNamedCollectionPtr collection,
    std::lock_guard<std::mutex> &)
{
    auto [it, inserted] = loaded_named_collections.emplace(collection_name, collection);
    if (!inserted)
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
            "A named collection `{}` already exists",
            collection_name);
    }
    CurrentMetrics::set(CurrentMetrics::NamedCollection, loaded_named_collections.size());
}

void NamedCollectionFactory::add(NamedCollectionsMap collections, std::lock_guard<std::mutex> & lock)
{
    for (const auto & [collection_name, collection] : collections)
        add(collection_name, collection, lock);
}

void NamedCollectionFactory::remove(const std::string & collection_name, std::lock_guard<std::mutex> & lock)
{
    bool removed = removeIfExists(collection_name, lock);
    if (!removed)
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "There is no named collection `{}`",
            collection_name);
    }
}

bool NamedCollectionFactory::removeIfExists(
    const std::string & collection_name,
    std::lock_guard<std::mutex> & lock)
{
    auto collection = tryGet(collection_name, lock);
    if (!collection)
        return false;

    if (!collection->isMutable())
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_IS_IMMUTABLE,
            "Cannot get collection `{}` for modification, "
            "because collection was defined as immutable",
            collection_name);
    }
    loaded_named_collections.erase(collection_name);
    CurrentMetrics::set(CurrentMetrics::NamedCollection, loaded_named_collections.size());
    return true;
}

void NamedCollectionFactory::removeById(NamedCollection::SourceId id, std::lock_guard<std::mutex> &)
{
    std::erase_if(
        loaded_named_collections,
        [&](const auto & value) { return value.second->getSourceId() == id; });
    CurrentMetrics::set(CurrentMetrics::NamedCollection, loaded_named_collections.size());
}

namespace
{
    constexpr auto NAMED_COLLECTIONS_CONFIG_PREFIX = "named_collections";

    std::vector<std::string> listCollections(const Poco::Util::AbstractConfiguration & config)
    {
        Poco::Util::AbstractConfiguration::Keys collections_names;
        config.keys(NAMED_COLLECTIONS_CONFIG_PREFIX, collections_names);
        return collections_names;
    }

    MutableNamedCollectionPtr getCollection(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & collection_name)
    {
        const auto collection_prefix = fmt::format("{}.{}", NAMED_COLLECTIONS_CONFIG_PREFIX, collection_name);
        std::queue<std::string> enumerate_input;
        std::set<std::string, std::less<>> enumerate_result;

        enumerate_input.push(collection_prefix);
        NamedCollectionConfiguration::listKeys(config, std::move(enumerate_input), enumerate_result, -1);

        /// Collection does not have any keys. (`enumerate_result` == <collection_path>).
        const bool collection_is_empty = enumerate_result.size() == 1
            && *enumerate_result.begin() == collection_prefix;

        std::set<std::string, std::less<>> keys;
        if (!collection_is_empty)
        {
            /// Skip collection prefix and add +1 to avoid '.' in the beginning.
            for (const auto & path : enumerate_result)
                keys.emplace(path.substr(collection_prefix.size() + 1));
        }

        return NamedCollectionFromConfig::create(
            config, collection_name, collection_prefix, keys);
    }

    NamedCollectionsMap getNamedCollections(const Poco::Util::AbstractConfiguration & config)
    {
        NamedCollectionsMap result;
        for (const auto & collection_name : listCollections(config))
        {
            if (result.contains(collection_name))
            {
                throw Exception(
                    ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
                    "Found duplicate named collection `{}`",
                    collection_name);
            }
            result.emplace(collection_name, getCollection(config, collection_name));
        }
        return result;
    }
}

void NamedCollectionFactory::loadIfNot()
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
}

bool NamedCollectionFactory::loadIfNot(std::lock_guard<std::mutex> & lock)
{
    if (loaded)
        return false;

    auto context = Context::getGlobalContextInstance();
    metadata_storage = NamedCollectionsMetadataStorage::create(context);

    loadFromConfig(context->getConfigRef(), lock);
    loadFromSQL(lock);

    if (metadata_storage->isReplicated())
    {
        update_task = context->getSchedulePool()->createTask(StorageID::createEmpty(), "NamedCollectionsMetadataStorage", [this]{ updateFunc(); });
        update_task->activate();
        update_task->schedule();
    }

    loaded = true;
    return true;
}

void NamedCollectionFactory::loadFromConfig(const Poco::Util::AbstractConfiguration & config, std::lock_guard<std::mutex> & lock)
{
    auto collections = getNamedCollections(config);
    LOG_TEST(log, "Loaded {} collections from config", collections.size());
    add(std::move(collections), lock);
}

void NamedCollectionFactory::reloadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(mutex);
    if (loadIfNot(lock))
        return;

    auto collections = getNamedCollections(config);
    LOG_TEST(log, "Loaded {} collections from config", collections.size());

    removeById(NamedCollection::SourceId::CONFIG, lock);
    add(std::move(collections), lock);
}

void NamedCollectionFactory::loadFromSQL(std::lock_guard<std::mutex> & lock)
{
    auto collections = metadata_storage->getAll();
    LOG_TEST(log, "Loaded {} collections from sql", collections.size());
    add(std::move(collections), lock);
}

void NamedCollectionFactory::createFromSQL(const ASTCreateNamedCollectionQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);

    if (exists(query.collection_name, lock))
    {
        if (query.if_not_exists)
            return;

        throw Exception(
            ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
            "A named collection `{}` already exists",
            query.collection_name);
    }

    add(query.collection_name, metadata_storage->create(query), lock);
}

void NamedCollectionFactory::removeFromSQL(const ASTDropNamedCollectionQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);

    if (!exists(query.collection_name, lock))
    {
        if (query.if_exists)
            return;

        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "Cannot remove collection `{}`, because it doesn't exist",
            query.collection_name);
    }

    metadata_storage->remove(query.collection_name);
    remove(query.collection_name, lock);
}

bool NamedCollectionFactory::removeFromSQLIfNoDependencies(const ASTDropNamedCollectionQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);

    const auto detached_it = detached_dependencies.lower_bound(std::make_tuple(query.collection_name, String{}, String{}));
    if (dependencies.get<Collection>().find(query.collection_name) != dependencies.get<Collection>().end()
        || (detached_it != detached_dependencies.end() && std::get<0>(*detached_it) == query.collection_name))
        return false;

    if (!exists(query.collection_name, lock))
    {
        if (query.if_exists)
            return true;

        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "Cannot remove collection `{}`, because it doesn't exist",
            query.collection_name);
    }

    metadata_storage->remove(query.collection_name);
    remove(query.collection_name, lock);
    return true;
}

void NamedCollectionFactory::updateFromSQL(const ASTAlterNamedCollectionQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);

    auto collection_name = query.collection_name;
    if (!exists(collection_name, lock))
    {
        if (query.if_exists)
            return;

        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "Cannot update collection `{}`, because it doesn't exist",
            collection_name);
    }
    auto updated_collection_ptr = metadata_storage->update(query);

    auto it = loaded_named_collections.find(collection_name);
    if (it == loaded_named_collections.end())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The named collection {} unexpectedly does not exist.",
            collection_name);
    }

    if (!it->second->isMutable())
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_IS_IMMUTABLE,
            "Cannot get collection `{}` for modification, "
            "because collection was defined as immutable",
            collection_name);
    }
    it->second = updated_collection_ptr;
}

void NamedCollectionFactory::reloadFromSQL()
{
    std::lock_guard lock(mutex);
    if (loadIfNot(lock))
        return;

    auto collections = metadata_storage->getAll();
    removeById(NamedCollection::SourceId::SQL, lock);
    add(std::move(collections), lock);
}

bool NamedCollectionFactory::usesReplicatedStorage()
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    return metadata_storage->isReplicated();
}

void NamedCollectionFactory::updateFunc()
{
    LOG_TRACE(log, "Named collections background updating thread started");

    while (!shutdown_called.load())
    {
        try
        {
            if (metadata_storage->waitUpdate())
            {
                reloadFromSQL();
            }
        }
        catch (const Coordination::Exception & e)
        {
            if (Coordination::isHardwareError(e.code))
            {
                LOG_INFO(log, "Lost ZooKeeper connection, will try to connect again: {}",
                        DB::getCurrentExceptionMessage(true));

                sleepForSeconds(1);
            }
            else
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                chassert(false);
            }
            continue;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            chassert(false);
            continue;
        }
    }

    LOG_TRACE(log, "Named collections background updating thread finished");
}

namespace
{

/// The entries registered for a table of an `Atomic` database. They are found by the UUID, which the
/// table keeps across a `RENAME` while the name stored next to the entry goes stale. A UUID can also
/// be reused by `CREATE TABLE ... UUID` after a failed create, which can leave an entry of a table
/// that never came to exist under another name: the entries of the current name are preferred,
/// because they certainly belong to this table. Only when the table has no entry under its current
/// name can an entry of another name be its own - one registered before a rename.
template <typename Index>
std::vector<typename Index::iterator> findEntriesByUUID(Index & idx, const StorageID & table_id)
{
    std::vector<typename Index::iterator> entries_of_this_name;
    std::vector<typename Index::iterator> all_entries;

    auto range = idx.equal_range(table_id.uuid);
    for (auto it = range.first; it != range.second; ++it)
    {
        all_entries.push_back(it);
        if (it->table_id.database_name == table_id.database_name && it->table_id.table_name == table_id.table_name)
            entries_of_this_name.push_back(it);
    }

    return entries_of_this_name.empty() ? all_entries : entries_of_this_name;
}

/// A dependency of a database engine is recorded with an empty table name, and the accessors of
/// `StorageID` reject such an identifier, so it cannot be formatted the usual way.
String getDependencyNameForLogs(const StorageID & table_id)
{
    if (table_id.table_name.empty())
        return fmt::format("database {}", backQuoteIfNeed(table_id.database_name));
    return table_id.getNameForLogs();
}

}

void NamedCollectionFactory::addDependency(const String & collection_name, const StorageID & table_id)
{
    std::lock_guard lock(mutex);
    LOG_TRACE(log, "Adding dependency: collection={}, dependent={}", collection_name, getDependencyNameForLogs(table_id));

    /// The same dependency can be registered more than once for one table - a lazily loaded table
    /// registers it from its metadata and again when its proxy is materialized - and a duplicate would
    /// only make the same table be listed twice in the error message of `DROP NAMED COLLECTION`.
    const auto & idx = dependencies.get<Collection>();
    auto range = idx.equal_range(collection_name);
    for (auto it = range.first; it != range.second; ++it)
    {
        if (it->table_id.database_name == table_id.database_name && it->table_id.table_name == table_id.table_name
            && it->table_id.uuid == table_id.uuid)
            return;
    }

    dependencies.emplace(collection_name, table_id);

    /// The detached entry of the table, if any, is deliberately not removed here: the dependencies are
    /// registered while the engine arguments are resolved, and the `ATTACH` can still fail after that,
    /// leaving the table detached. The entry is removed only when the table is dropped, detached
    /// permanently, or renamed.
}

NamedCollectionPtr NamedCollectionFactory::getAndAddDependency(
    const String & collection_name,
    bool throw_unknown_collection,
    const StorageID & table_id)
{
    std::lock_guard lock(mutex);
    auto collection = tryGet(collection_name, lock);
    if (!collection)
    {
        if (throw_unknown_collection)
            throw Exception(ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST, "There is no named collection `{}`", collection_name);
        return nullptr;
    }

    auto & dependencies_by_collection = dependencies.get<Collection>();
    auto range = dependencies_by_collection.equal_range(collection_name);
    for (auto it = range.first; it != range.second; ++it)
    {
        if (it->table_id.database_name == table_id.database_name && it->table_id.table_name == table_id.table_name
            && it->table_id.uuid == table_id.uuid)
            return collection;
    }

    dependencies.emplace(collection_name, table_id);
    return collection;
}

void NamedCollectionFactory::removeDependencies(const StorageID & table_id)
{
    std::lock_guard lock(mutex);

    if (table_id.hasUUID())
    {
        auto & idx = dependencies.get<TableUUID>();
        for (auto it : findEntriesByUUID(idx, table_id))
            idx.erase(it);
    }
    else
    {
        /// Remove by name for non-Atomic databases (Ordinary, etc.) which don't have UUIDs.
        /// We only remove entries that have Nil UUIDs - entries with UUIDs belong to Atomic
        /// databases and must be removed via the UUID index (handled in the if branch above).
        auto & idx = dependencies.get<TableName>();
        auto range = idx.equal_range(std::make_tuple(table_id.database_name, table_id.table_name));

        /// Collect entries to erase - only those without UUIDs (non-Atomic database entries)
        std::vector<decltype(range.first)> to_erase;
        for (auto it = range.first; it != range.second; ++it)
        {
            if (it->table_id.uuid == UUIDHelpers::Nil)
                to_erase.push_back(it);
        }

        for (auto it : to_erase)
            idx.erase(it);
    }
}

void NamedCollectionFactory::removeDependency(const String & collection_name, const StorageID & table_id)
{
    std::lock_guard lock(mutex);

    auto & idx = dependencies.get<Collection>();
    auto range = idx.equal_range(collection_name);
    for (auto it = range.first; it != range.second; ++it)
    {
        if (it->table_id.database_name == table_id.database_name && it->table_id.table_name == table_id.table_name
            && it->table_id.uuid == table_id.uuid)
        {
            idx.erase(it);
            return;
        }
    }
}

void NamedCollectionFactory::renameDependencies(const StorageID & from_table_id, const StorageID & to_table_id, bool exchange)
{
    std::lock_guard lock(mutex);

    /// Only an attached table can be renamed, so a detached entry recorded under its name is a leftover
    /// of an earlier detach: erase it, the metadata file under the old name is gone after the rename.
    std::erase_if(
        detached_dependencies,
        [&](const auto & entry)
        { return std::get<1>(entry) == from_table_id.database_name && std::get<2>(entry) == from_table_id.table_name; });

    /// The rename interpreter passes StorageIDs without UUIDs.
    /// If either ID has a UUID, it's not a standard rename operation, so nothing to do.
    if (from_table_id.hasUUID() || to_table_id.hasUUID())
        return;

    /// Move the entries recorded under the exact old name to the new one. For a table of an `Ordinary`
    /// database (no UUID in the entry) the name is the identity itself. For a table of an `Atomic`
    /// database the identity is the UUID, which the rename keeps, but the recorded name still
    /// disambiguates a `CREATE TABLE ... UUID` that reused the UUID of a failed create (see the
    /// stale-dependency cleanup of `DROP NAMED COLLECTION`), so it must follow the rename: an entry
    /// under another name than the table's current one is exactly the leftover of such a failed create.
    /// An entry of a different table under the same name cannot be moved by mistake: a live entry
    /// carries its table's current name, and the rename source name has no other live owner.
    /// An `EXCHANGE` calls this once per direction, and re-keying UUID entries by name would apply the
    /// second call to the entries the first one just moved, so there the exchanged tables keep their
    /// old entry names: the drop check resolves them by UUID and keeps refusing the drop.
    auto & name_idx = dependencies.get<TableName>();
    auto name_range = name_idx.equal_range(std::make_tuple(from_table_id.database_name, from_table_id.table_name));

    /// A single table can depend on several named collections, so all the entries of the name are
    /// re-inserted with the new one.
    std::vector<std::pair<String, UUID>> moved;
    std::vector<decltype(name_range.first)> to_erase;
    for (auto it = name_range.first; it != name_range.second; ++it)
    {
        if (exchange && it->table_id.uuid != UUIDHelpers::Nil)
            continue;

        moved.emplace_back(it->collection_name, it->table_id.uuid);
        to_erase.push_back(it);
    }

    for (auto it : to_erase)
        name_idx.erase(it);

    for (const auto & [collection_name, uuid] : moved)
        dependencies.emplace(collection_name, StorageID{to_table_id.database_name, to_table_id.table_name, uuid});
}

void NamedCollectionFactory::rekeyDependencies(const StorageID & from_table_id, const StorageID & to_table_id)
{
    std::lock_guard lock(mutex);

    LOG_TRACE(
        log,
        "Re-keying dependencies of {} to {}",
        getDependencyNameForLogs(from_table_id),
        getDependencyNameForLogs(to_table_id));

    std::vector<String> collection_names;

    /// The table is identified by its UUID while it belongs to an `Atomic` database and by its name
    /// while it belongs to an `Ordinary` one, so exactly one of the two lookups can find the entries.
    if (from_table_id.hasUUID())
    {
        auto & uuid_idx = dependencies.get<TableUUID>();
        for (auto it : findEntriesByUUID(uuid_idx, from_table_id))
        {
            collection_names.push_back(it->collection_name);
            uuid_idx.erase(it);
        }
    }
    else
    {
        /// Like in `removeDependencies`: entries with UUIDs belong to tables of `Atomic` databases.
        auto & name_idx = dependencies.get<TableName>();
        auto range = name_idx.equal_range(std::make_tuple(from_table_id.database_name, from_table_id.table_name));

        std::vector<decltype(range.first)> to_erase;
        for (auto it = range.first; it != range.second; ++it)
        {
            if (it->table_id.uuid == UUIDHelpers::Nil)
            {
                collection_names.push_back(it->collection_name);
                to_erase.push_back(it);
            }
        }

        for (auto it : to_erase)
            name_idx.erase(it);
    }

    for (const auto & collection_name : collection_names)
        dependencies.emplace(collection_name, to_table_id);
}

bool NamedCollectionFactory::hasDependencyRegisteredFor(const StorageID & table_id) const
{
    std::lock_guard lock(mutex);

    const auto & idx = dependencies.get<TableName>();
    auto range = idx.equal_range(std::make_tuple(table_id.database_name, table_id.table_name));

    for (auto it = range.first; it != range.second; ++it)
    {
        if (it->table_id.uuid == table_id.uuid)
            return true;
    }

    return false;
}

std::vector<StorageID> NamedCollectionFactory::getDependents(const String & collection_name) const
{
    std::lock_guard lock(mutex);
    std::vector<StorageID> result;

    const auto & idx = dependencies.get<Collection>();
    auto range = idx.equal_range(collection_name);

    for (auto it = range.first; it != range.second; ++it)
        result.push_back(it->table_id);

    return result;
}

void NamedCollectionFactory::markDependenciesDetached(const StorageID & table_id)
{
    std::lock_guard lock(mutex);
    LOG_TRACE(log, "Marking dependencies of {} as detached", getDependencyNameForLogs(table_id));

    std::vector<String> collection_names;

    if (table_id.hasUUID())
    {
        auto & idx = dependencies.get<TableUUID>();
        for (auto it : findEntriesByUUID(idx, table_id))
        {
            collection_names.push_back(it->collection_name);
            idx.erase(it);
        }
    }
    else
    {
        /// Like in `removeDependencies`: entries with UUIDs belong to tables of `Atomic` databases and
        /// are handled through the UUID index above.
        auto & idx = dependencies.get<TableName>();
        auto range = idx.equal_range(std::make_tuple(table_id.database_name, table_id.table_name));

        std::vector<decltype(range.first)> to_erase;
        for (auto it = range.first; it != range.second; ++it)
        {
            if (it->table_id.uuid == UUIDHelpers::Nil)
            {
                collection_names.push_back(it->collection_name);
                to_erase.push_back(it);
            }
        }

        for (auto it : to_erase)
            idx.erase(it);
    }

    /// The current names, not the ones stored in the dependency (which go stale on `RENAME`), and not
    /// the UUID: the entry is removed when a table with this name is attached, and the metadata file the
    /// detached table would be attached from is named after the current name.
    for (const auto & collection_name : collection_names)
        detached_dependencies.insert({collection_name, table_id.database_name, table_id.table_name});
}

std::vector<StorageID> NamedCollectionFactory::getDetachedDependents(const String & collection_name) const
{
    std::lock_guard lock(mutex);
    std::vector<StorageID> result;

    for (auto it = detached_dependencies.lower_bound(std::make_tuple(collection_name, String{}, String{}));
         it != detached_dependencies.end() && std::get<0>(*it) == collection_name;
         ++it)
    {
        const auto & database_name = std::get<1>(*it);
        const auto & table_name = std::get<2>(*it);
        /// An entry of a detached database engine has no table name.
        if (table_name.empty())
            result.push_back(StorageID::createDatabaseOnly(database_name));
        else
            result.push_back(StorageID{database_name, table_name});
    }

    return result;
}

void NamedCollectionFactory::removeDetachedDependencies(const StorageID & table_id)
{
    std::lock_guard lock(mutex);
    std::erase_if(
        detached_dependencies,
        [&](const auto & entry)
        { return std::get<1>(entry) == table_id.database_name && std::get<2>(entry) == table_id.table_name; });
}

void NamedCollectionFactory::removeDetachedDependencies(const String & database_name)
{
    std::lock_guard lock(mutex);
    std::erase_if(detached_dependencies, [&](const auto & entry) { return std::get<1>(entry) == database_name; });
}

void NamedCollectionFactory::renameDetachedDependencies(const String & from_database_name, const String & to_database_name)
{
    std::lock_guard lock(mutex);

    std::vector<std::tuple<String, String, String>> renamed;
    for (auto it = detached_dependencies.begin(); it != detached_dependencies.end();)
    {
        if (std::get<1>(*it) == from_database_name)
        {
            renamed.emplace_back(std::get<0>(*it), to_database_name, std::get<2>(*it));
            it = detached_dependencies.erase(it);
        }
        else
            ++it;
    }

    detached_dependencies.insert(renamed.begin(), renamed.end());
}

}
