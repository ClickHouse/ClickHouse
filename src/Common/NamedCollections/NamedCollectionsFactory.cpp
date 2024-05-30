#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/NamedCollections/NamedCollectionConfiguration.h>
#include <Common/NamedCollections/NamedCollectionsMetadata.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NAMED_COLLECTION_DOESNT_EXIST;
    extern const int NAMED_COLLECTION_ALREADY_EXISTS;
    extern const int NAMED_COLLECTION_IS_IMMUTABLE;
}

NamedCollectionFactory & NamedCollectionFactory::instance()
{
    static NamedCollectionFactory instance;
    return instance;
}

bool NamedCollectionFactory::exists(const std::string & collection_name) const
{
    std::lock_guard lock(mutex);
    return existsUnlocked(collection_name, lock);
}

bool NamedCollectionFactory::existsUnlocked(
    const std::string & collection_name,
    std::lock_guard<std::mutex> & /* lock */) const
{
    return loaded_named_collections.contains(collection_name);
}

NamedCollectionPtr NamedCollectionFactory::get(const std::string & collection_name) const
{
    std::lock_guard lock(mutex);
    auto collection = tryGetUnlocked(collection_name, lock);
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
    return tryGetUnlocked(collection_name, lock);
}

MutableNamedCollectionPtr NamedCollectionFactory::getMutable(
    const std::string & collection_name) const
{
    std::lock_guard lock(mutex);
    auto collection = tryGetUnlocked(collection_name, lock);
    if (!collection)
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "There is no named collection `{}`",
            collection_name);
    }
    else if (!collection->isMutable())
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_IS_IMMUTABLE,
            "Cannot get collection `{}` for modification, "
            "because collection was defined as immutable",
            collection_name);
    }
    return collection;
}

MutableNamedCollectionPtr NamedCollectionFactory::tryGetUnlocked(
    const std::string & collection_name,
    std::lock_guard<std::mutex> & /* lock */) const
{
    auto it = loaded_named_collections.find(collection_name);
    if (it == loaded_named_collections.end())
        return nullptr;
    return it->second;
}

void NamedCollectionFactory::add(
    const std::string & collection_name,
    MutableNamedCollectionPtr collection)
{
    std::lock_guard lock(mutex);
    addUnlocked(collection_name, collection, lock);
}

void NamedCollectionFactory::add(NamedCollectionsMap collections)
{
    std::lock_guard lock(mutex);
    for (const auto & [collection_name, collection] : collections)
        addUnlocked(collection_name, collection, lock);
}

void NamedCollectionFactory::addUnlocked(
    const std::string & collection_name,
    MutableNamedCollectionPtr collection,
    std::lock_guard<std::mutex> & /* lock */)
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

void NamedCollectionFactory::remove(const std::string & collection_name)
{
    std::lock_guard lock(mutex);
    bool removed = removeIfExistsUnlocked(collection_name, lock);
    if (!removed)
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "There is no named collection `{}`",
            collection_name);
    }
}

void NamedCollectionFactory::removeIfExists(const std::string & collection_name)
{
    std::lock_guard lock(mutex);
    removeIfExistsUnlocked(collection_name, lock); // NOLINT
}

bool NamedCollectionFactory::removeIfExistsUnlocked(
    const std::string & collection_name,
    std::lock_guard<std::mutex> & lock)
{
    auto collection = tryGetUnlocked(collection_name, lock);
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

void NamedCollectionFactory::removeById(NamedCollection::SourceId id)
{
    std::lock_guard lock(mutex);
    std::erase_if(
        loaded_named_collections,
        [&](const auto & value) { return value.second->getSourceId() == id; });
}

NamedCollectionsMap NamedCollectionFactory::getAll() const
{
    std::lock_guard lock(mutex);
    return loaded_named_collections;
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

        return NamedCollection::create(
            config, collection_name, collection_prefix, keys, NamedCollection::SourceId::CONFIG, /* is_mutable */false);
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

void NamedCollectionFactory::loadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    add(getNamedCollections(config));
}

void NamedCollectionFactory::reloadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    auto collections = getNamedCollections(config);
    removeById(NamedCollection::SourceId::CONFIG);
    add(collections);
}

void NamedCollectionFactory::loadFromSQL(const ContextPtr & context)
{
    add(NamedCollectionsMetadata::create(context)->getAll());
}

void NamedCollectionFactory::loadIfNot()
{
    if (loaded)
        return;
    auto global_context = Context::getGlobalContextInstance();
    loadFromConfig(global_context->getConfigRef());
    loadFromSQL(global_context);
    loaded = true;
}

void NamedCollectionFactory::createFromSQL(const ASTCreateNamedCollectionQuery & query, ContextPtr context)
{
    loadIfNot();
    if (exists(query.collection_name))
    {
        if (query.if_not_exists)
            return;

        throw Exception(
            ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
            "A named collection `{}` already exists",
            query.collection_name);
    }
    add(query.collection_name, NamedCollectionsMetadata::create(context)->create(query));
}

void NamedCollectionFactory::removeFromSQL(const ASTDropNamedCollectionQuery & query, ContextPtr context)
{
    loadIfNot();
    if (!exists(query.collection_name))
    {
        if (query.if_exists)
            return;

        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "Cannot remove collection `{}`, because it doesn't exist",
            query.collection_name);
    }
    NamedCollectionsMetadata::create(context)->remove(query.collection_name);
    remove(query.collection_name);
}

void NamedCollectionFactory::updateFromSQL(const ASTAlterNamedCollectionQuery & query, ContextPtr context)
{
    loadIfNot();
    if (!exists(query.collection_name))
    {
        if (query.if_exists)
            return;

        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "Cannot remove collection `{}`, because it doesn't exist",
            query.collection_name);
    }
    NamedCollectionsMetadata::create(context)->update(query);

    auto collection = getMutable(query.collection_name);
    auto collection_lock = collection->lock();

    for (const auto & [name, value] : query.changes)
    {
        auto it_override = query.overridability.find(name);
        if (it_override != query.overridability.end())
            collection->setOrUpdate<String, true>(name, convertFieldToString(value), it_override->second);
        else
            collection->setOrUpdate<String, true>(name, convertFieldToString(value), {});
    }

    for (const auto & key : query.delete_keys)
        collection->remove<true>(key);
}

}
