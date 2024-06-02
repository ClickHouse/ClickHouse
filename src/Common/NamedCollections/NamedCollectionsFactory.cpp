#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/NamedCollections/NamedCollectionUtils.h>

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

void NamedCollectionFactory::removeById(NamedCollectionUtils::SourceId id)
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

}
