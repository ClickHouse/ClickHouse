#pragma once
#include <Common/NamedCollections/NamedCollections.h>

namespace DB
{

class NamedCollectionFactory : boost::noncopyable
{
public:
    static NamedCollectionFactory & instance();

    bool exists(const std::string & collection_name) const;

    NamedCollectionPtr get(const std::string & collection_name) const;

    NamedCollectionPtr tryGet(const std::string & collection_name) const;

    MutableNamedCollectionPtr getMutable(const std::string & collection_name) const;

    void add(const std::string & collection_name, MutableNamedCollectionPtr collection);

    void add(NamedCollectionsMap collections);

    void update(NamedCollectionsMap collections);

    void remove(const std::string & collection_name);

    void removeIfExists(const std::string & collection_name);

    void removeById(NamedCollectionUtils::SourceId id);

    NamedCollectionsMap getAll() const;

private:
    bool existsUnlocked(
        const std::string & collection_name,
        std::lock_guard<std::mutex> & lock) const;

    MutableNamedCollectionPtr tryGetUnlocked(
        const std::string & collection_name,
        std::lock_guard<std::mutex> & lock) const;

    void addUnlocked(
        const std::string & collection_name,
        MutableNamedCollectionPtr collection,
        std::lock_guard<std::mutex> & lock);

    bool removeIfExistsUnlocked(
        const std::string & collection_name,
        std::lock_guard<std::mutex> & lock);

    mutable NamedCollectionsMap loaded_named_collections;

    mutable std::mutex mutex;
    bool is_initialized = false;
};

}
