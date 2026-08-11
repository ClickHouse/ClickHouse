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

#include <set>
#include <tuple>

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
    /// Removes one exact entry: the collection and the whole `StorageID` (database, table, UUID) must
    /// match. The stale-entry cleanup of `DROP NAMED COLLECTION` uses it because the proof of staleness
    /// it holds - the `DDLGuard` of the recorded table name - covers only that exact entry: erasing
    /// everything under the entry's UUID could remove the live dependency of an in-flight
    /// `CREATE TABLE ... UUID` that reuses the UUID under a different table name.
    void removeDependency(const String & collection_name, const StorageID & table_id);
    void renameDependencies(const StorageID & from_table_id, const StorageID & to_table_id);
    /// A `RENAME TABLE` that moves a table between an `Ordinary` and an `Atomic` database changes the
    /// identity the dependency is keyed by: the move into `Atomic` assigns a fresh UUID to the table and
    /// the move out of it drops the UUID, while the rename interpreter only knows the names. Re-key the
    /// entries of the moved table to its new `StorageID` so that a later `DETACH`, `DROP` or `RENAME`
    /// still finds them.
    void rekeyDependencies(const StorageID & from_table_id, const StorageID & to_table_id);
    std::vector<StorageID> getDependents(const String & collection_name) const;

    /// `DETACH TABLE` moves the dependencies of the table here: a detached table is not in
    /// `DatabaseCatalog`, but the metadata it is attached from still references the collections, so they
    /// must not be dropped. An entry is removed only when the table is dropped, detached permanently, or
    /// renamed (all of which require the table to have been attached back first), and when its database
    /// is dropped. `ATTACH` itself does not remove the entry: the dependencies are registered again while
    /// the engine arguments are resolved, but the attach can still fail after that, leaving the table
    /// detached. The `DROP NAMED COLLECTION` check does not prune the entries of tables that exist in
    /// `DatabaseCatalog` either: the table's existence there is racy against in-flight `ATTACH`/`DETACH`
    /// and proves nothing about whether the drop validated the live dependency.
    /// The list is kept in memory only, which is consistent across a restart: a plainly detached table
    /// is attached again at the next start (its dependencies are registered normally), and a permanently
    /// detached table does not record entries at all - it is not loaded at startup, so a dropped
    /// collection cannot break the start. The list is deliberately imprecise: an entry may keep refusing
    /// the drop after the table was attached back (until the table is dropped or renamed) or after the
    /// detached table itself is gone.
    void markDependenciesDetached(const StorageID & table_id);
    /// The detached tables that referenced the collection when they were detached.
    std::vector<StorageID> getDetachedDependents(const String & collection_name) const;
    /// `DROP TABLE` and `DETACH TABLE ... PERMANENTLY` remove the metadata the entry stands for.
    void removeDetachedDependencies(const StorageID & table_id);
    /// `DROP DATABASE` drops the detached tables of the database too: forget about them.
    void removeDetachedDependencies(const String & database_name);
    /// `RENAME DATABASE` moves the metadata of its detached tables along: re-key their entries.
    void renameDetachedDependencies(const String & from_database_name, const String & to_database_name);

protected:
    mutable NamedCollectionsMap loaded_named_collections;
    mutable std::mutex mutex;
    NamedCollectionDependencies dependencies;
    /// (collection name, database name, table name) of the dependencies of detached tables.
    std::set<std::tuple<String, String, String>> detached_dependencies;

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
