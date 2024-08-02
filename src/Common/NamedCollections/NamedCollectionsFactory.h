#pragma once
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionsMetadataStorage.h>
#include <Common/logger_useful.h>

namespace DB
{
class ASTCreateNamedCollectionQuery;
class ASTDropNamedCollectionQuery;
class ASTAlterNamedCollectionQuery;

class NamedCollectionFactory : boost::noncopyable
{
public:
    static NamedCollectionFactory & instance();

    ~NamedCollectionFactory();

    bool exists(const std::string & collection_name) const;

    NamedCollectionPtr get(const std::string & collection_name) const;

    NamedCollectionPtr tryGet(const std::string & collection_name) const;

    NamedCollectionsMap getAll() const;

    void reloadFromConfig(const Poco::Util::AbstractConfiguration & config);

    void reloadFromSQL();

    void createFromSQL(const ASTCreateNamedCollectionQuery & query);

    void removeFromSQL(const ASTDropNamedCollectionQuery & query);

    void updateFromSQL(const ASTAlterNamedCollectionQuery & query);

    bool usesReplicatedStorage();

    void loadIfNot();

    void shutdown();

protected:
    mutable NamedCollectionsMap loaded_named_collections;
    mutable std::mutex mutex;

    const LoggerPtr log = getLogger("NamedCollectionFactory");

    bool loaded = false;
    std::atomic<bool> shutdown_called = false;
    std::unique_ptr<NamedCollectionsMetadataStorage> metadata_storage;
    BackgroundSchedulePool::TaskHolder update_task;

    bool loadIfNot(std::lock_guard<std::mutex> & lock);

    bool exists(
        const std::string & collection_name,
        std::lock_guard<std::mutex> & lock) const;

    MutableNamedCollectionPtr getMutable(const std::string & collection_name, std::lock_guard<std::mutex> & lock) const;

    void add(const std::string & collection_name, MutableNamedCollectionPtr collection, std::lock_guard<std::mutex> & lock);

    void add(NamedCollectionsMap collections, std::lock_guard<std::mutex> & lock);

    void update(NamedCollectionsMap collections, std::lock_guard<std::mutex> & lock);

    void remove(const std::string & collection_name, std::lock_guard<std::mutex> & lock);

    bool removeIfExists(const std::string & collection_name, std::lock_guard<std::mutex> & lock);

    MutableNamedCollectionPtr tryGet(const std::string & collection_name, std::lock_guard<std::mutex> & lock) const;

    void removeById(NamedCollection::SourceId id, std::lock_guard<std::mutex> & lock);

    void loadFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        std::lock_guard<std::mutex> & lock);

    void loadFromSQL(std::lock_guard<std::mutex> & lock);

    void updateFunc();
};

}
