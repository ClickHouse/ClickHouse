#include <Core/Settings.h>
#include <base/sleep.h>
#include <Common/FieldVisitorToString.h>
#include <Common/NamedCollections/NamedCollectionConfiguration.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/NamedCollections/NamedCollectionsMetadataStorage.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/BackgroundSchedulePool.h>
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
    return true;
}

void NamedCollectionFactory::removeById(NamedCollection::SourceId id, std::lock_guard<std::mutex> &)
{
    std::erase_if(
        loaded_named_collections,
        [&](const auto & value) { return value.second->getSourceId() == id; });
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
        update_task = context->getSchedulePool().createTask(StorageID::createEmpty(), "NamedCollectionsMetadataStorage", [this]{ updateFunc(); });
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
    CurrentMetrics::sub(CurrentMetrics::NamedCollection);
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

void NamedCollectionFactory::addDependency(const String & collection_name, const StorageID & table_id)
{
    std::lock_guard lock(mutex);
    LOG_TRACE(log, "Adding dependency: collection={}, table={}", collection_name, table_id.getNameForLogs());
    dependencies.emplace(collection_name, table_id);
}

void NamedCollectionFactory::removeDependencies(const StorageID & table_id)
{
    std::lock_guard lock(mutex);

    if (table_id.hasUUID())
    {
        /// Remove by UUID - this is the most reliable method for Atomic databases
        auto & idx = dependencies.get<TableUUID>();
        idx.erase(table_id.uuid);
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

void NamedCollectionFactory::renameDependencies(const StorageID & from_table_id, const StorageID & to_table_id)
{
    std::lock_guard lock(mutex);

    /// The rename interpreter passes StorageIDs without UUIDs.
    /// If either ID has a UUID, it's not a standard rename operation, so nothing to do.
    /// For Atomic databases, the dependencies are tracked by UUID which doesn't change on rename.
    if (from_table_id.hasUUID() || to_table_id.hasUUID())
        return;

    /// For non-Atomic databases (name-based tracking), we need to update the table name.
    /// But we should only update entries that don't have UUIDs - entries with UUIDs
    /// are from Atomic databases and should remain unchanged (UUID-based lookup still works).
    auto & name_idx = dependencies.get<TableName>();
    auto name_range = name_idx.equal_range(std::make_tuple(from_table_id.database_name, from_table_id.table_name));

    /// Collect only entries without UUIDs (non-Atomic database entries).
    /// A single table can depend on several named collections, so we need to save all of them
    /// to re-insert the dependency with the new name.
    std::vector<String> collection_names;
    std::vector<decltype(name_range.first)> to_erase;
    for (auto it = name_range.first; it != name_range.second; ++it)
    {
        if (it->table_id.uuid == UUIDHelpers::Nil)
        {
            collection_names.push_back(it->collection_name);
            to_erase.push_back(it);
        }
    }

    if (to_erase.empty())
        return;

    /// Erase and re-insert with new table name
    for (auto it : to_erase)
        name_idx.erase(it);

    for (const auto & name : collection_names)
        dependencies.emplace(name, to_table_id);
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

}
