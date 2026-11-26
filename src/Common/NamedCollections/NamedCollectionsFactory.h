#pragma once

#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionsMetadataStorage.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Interpreters/StorageID.h>
#include <boost/noncopyable.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/mem_fun.hpp>

namespace DB
{
class ASTCreateNamedCollectionQuery;
class ASTDropNamedCollectionQuery;
class ASTAlterNamedCollectionQuery;

struct NamedCollectionDependency
{
    String collection_name;
    StorageID table_id;

    NamedCollectionDependency(const String & collection_name_, const StorageID & table_id_)
        : collection_name(collection_name_), table_id(table_id_) {}

    const String & getCollectionName() const { return collection_name; }
    const String & getDatabaseName() const { return table_id.database_name; }
    const String & getTableName() const { return table_id.table_name; }
    UUID getUUID() const { return table_id.uuid; }
};

struct Collection {};
struct TableUUID {};
struct TableName {};

using NamedCollectionDependencies = boost::multi_index_container<
    NamedCollectionDependency,
    boost::multi_index::indexed_by<
        boost::multi_index::hashed_non_unique<
            boost::multi_index::tag<Collection>,
            boost::multi_index::const_mem_fun<NamedCollectionDependency, const String &, &NamedCollectionDependency::getCollectionName>
        >,
        boost::multi_index::hashed_non_unique<
            boost::multi_index::tag<TableUUID>,
            boost::multi_index::const_mem_fun<NamedCollectionDependency, UUID, &NamedCollectionDependency::getUUID>,
            std::hash<UUID>
        >,
        /// For non-Atomic databases where tables don't have UUIDs
        boost::multi_index::hashed_non_unique<
            boost::multi_index::tag<TableName>,
            boost::multi_index::composite_key<
                NamedCollectionDependency,
                boost::multi_index::const_mem_fun<NamedCollectionDependency, const String &, &NamedCollectionDependency::getDatabaseName>,
                boost::multi_index::const_mem_fun<NamedCollectionDependency, const String &, &NamedCollectionDependency::getTableName>
            >
        >
    >
>;

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

    void addDependency(const String & collection_name, const StorageID & table_id);
    void removeDependencies(const StorageID & table_id);
    void renameDependencies(const StorageID & from_table_id, const StorageID & to_table_id);
    std::vector<StorageID> getDependents(const String & collection_name) const;

protected:
    mutable NamedCollectionsMap loaded_named_collections;
    mutable std::mutex mutex;
    NamedCollectionDependencies dependencies;

    const LoggerPtr log = getLogger("NamedCollectionFactory");

    bool loaded = false;
    std::atomic<bool> shutdown_called = false;
    std::unique_ptr<NamedCollectionsMetadataStorage> metadata_storage;
    BackgroundSchedulePoolTaskHolder update_task;

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
