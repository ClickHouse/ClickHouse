#pragma once

#include <Core/UUID.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Disks/IDisk.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/QueryFlags.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPipeline/BlockIO.h>
#include <Storages/IStorage_fwd.h>
#include <base/types.h>
#include <Common/AsyncLoader.h>

#include <ctime>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <vector>


namespace DB
{


struct Settings;
struct ConstraintsDescription;
struct IndicesDescription;
struct StorageInMemoryMetadata;
struct StorageID;
class ASTCreateQuery;
struct AlterCommand;
class AlterCommands;
class SettingsChanges;
using DictionariesWithID = std::vector<std::pair<String, UUID>>;
struct ParsedTablesMetadata;
struct QualifiedTableName;
class IRestoreCoordination;

class IDatabaseTablesIterator
{
public:
    virtual void next() = 0;
    virtual bool isValid() const = 0;

    virtual const String & name() const = 0;

    /// This method can return nullptr if it's Lazy database
    /// (a database with support for lazy tables loading
    /// - it maintains a list of tables but tables are loaded lazily).
    virtual const StoragePtr & table() const = 0;

    explicit IDatabaseTablesIterator(const String & database_name_) : database_name(database_name_) { }
    explicit IDatabaseTablesIterator(String && database_name_) : database_name(std::move(database_name_)) { }

    virtual ~IDatabaseTablesIterator() = default;

    virtual UUID uuid() const { return UUIDHelpers::Nil; }

    const String & databaseName() const { assert(!database_name.empty()); return database_name; }

protected:
    String database_name;
};

/// Copies list of tables and iterates through such snapshot.
class DatabaseTablesSnapshotIterator : public IDatabaseTablesIterator
{
private:
    Tables tables;
    Tables::iterator it;

protected:
    DatabaseTablesSnapshotIterator(DatabaseTablesSnapshotIterator && other) noexcept
    : IDatabaseTablesIterator(std::move(other.database_name))
    {
        size_t idx = std::distance(other.tables.begin(), other.it);
        std::swap(tables, other.tables);
        other.it = other.tables.end();
        it = tables.begin();
        std::advance(it, idx);
    }

public:
    DatabaseTablesSnapshotIterator(const Tables & tables_, const String & database_name_)
    : IDatabaseTablesIterator(database_name_), tables(tables_), it(tables.begin())
    {
    }

    DatabaseTablesSnapshotIterator(Tables && tables_, String && database_name_)
    : IDatabaseTablesIterator(std::move(database_name_)), tables(std::move(tables_)), it(tables.begin())
    {
    }

    void next() override { ++it; }

    bool isValid() const override { return it != tables.end(); }

    const String & name() const override { return it->first; }

    const StoragePtr & table() const override { return it->second; }
};

using DatabaseTablesIteratorPtr = std::unique_ptr<IDatabaseTablesIterator>;

struct SnapshotDetachedTable final
{
    String database;
    String table;
    UUID uuid = UUIDHelpers::Nil;
    String metadata_path;
    bool is_permanently{};
};

class DatabaseDetachedTablesSnapshotIterator
{
private:
    SnapshotDetachedTables snapshot;
    SnapshotDetachedTables::iterator it;

protected:
    DatabaseDetachedTablesSnapshotIterator(DatabaseDetachedTablesSnapshotIterator && other) noexcept
    {
        size_t idx = std::distance(other.snapshot.begin(), other.it);
        std::swap(snapshot, other.snapshot);
        other.it = other.snapshot.end();
        it = snapshot.begin();
        std::advance(it, idx);
    }

public:
    explicit DatabaseDetachedTablesSnapshotIterator(const SnapshotDetachedTables & tables_) : snapshot(tables_), it(snapshot.begin())
    {
    }

    explicit DatabaseDetachedTablesSnapshotIterator(SnapshotDetachedTables && tables_) : snapshot(std::move(tables_)), it(snapshot.begin())
    {
    }

    void next() { ++it; }

    bool isValid() const { return it != snapshot.end(); }

    String database() const { return it->second.database; }

    String table() const { return it->second.table; }

    UUID uuid() const { return it->second.uuid; }

    String metadataPath() const { return it->second.metadata_path; }

    bool isPermanently() const { return it->second.is_permanently; }
};

using DatabaseDetachedTablesSnapshotIteratorPtr = std::unique_ptr<DatabaseDetachedTablesSnapshotIterator>;


/** Database engine.
  * It is responsible for:
  * - initialization of set of known tables and dictionaries;
  * - checking existence of a table and getting a table object;
  * - retrieving a list of all tables;
  * - creating and dropping tables;
  * - renaming tables and moving between databases with same engine.
  */

class IDatabase : public std::enable_shared_from_this<IDatabase>
{
public:
    IDatabase() = delete;
    explicit IDatabase(String database_name_);

    /// Get name of database engine.
    virtual String getEngineName() const = 0;

    virtual bool canContainMergeTreeTables() const { return true; }

    virtual bool canContainDistributedTables() const { return true; }

    virtual bool canContainRocksDBTables() const { return true; }

    /// Load a set of existing tables.
    /// You can call only once, right after the object is created.
    virtual void loadStoredObjects( /// NOLINT
        ContextMutablePtr /*context*/,
        LoadingStrictnessLevel /*mode*/)
    {
    }

    virtual bool supportsLoadingInTopologicalOrder() const { return false; }

    virtual void beforeLoadingMetadata(
        ContextMutablePtr /*context*/, LoadingStrictnessLevel /*mode*/)
    {
    }

    virtual void loadTablesMetadata(ContextPtr /*local_context*/, ParsedTablesMetadata & /*metadata*/, bool /*is_startup*/);

    virtual void loadTableFromMetadata(
        ContextMutablePtr /*local_context*/,
        const String & /*file_path*/,
        const QualifiedTableName & /*name*/,
        const ASTPtr & /*ast*/,
        LoadingStrictnessLevel /*mode*/);

    /// Create a task to load table `name` after specified dependencies `startup_after` using `async_loader`.
    /// `load_after` must contain the tasks returned by `loadTableFromMetadataAsync()` for dependent tables (see TablesLoader).
    /// The returned task is also stored inside the database for cancellation on destruction.
    virtual LoadTaskPtr loadTableFromMetadataAsync(
        AsyncLoader & /*async_loader*/,
        LoadJobSet /*load_after*/,
        ContextMutablePtr /*local_context*/,
        const String & /*file_path*/,
        const QualifiedTableName & /*name*/,
        const ASTPtr & /*ast*/,
        LoadingStrictnessLevel /*mode*/);

    /// Create a task to startup table `name` after specified dependencies `startup_after` using `async_loader`.
    /// The returned task is also stored inside the database for cancellation on destruction.
    [[nodiscard]] virtual LoadTaskPtr startupTableAsync(
        AsyncLoader & /*async_loader*/,
        LoadJobSet /*startup_after*/,
        const QualifiedTableName & /*name*/,
        LoadingStrictnessLevel /*mode*/);

    /// Create a task to startup database after specified dependencies `startup_after` using `async_loader`.
    /// `startup_after` must contain all the tasks returned by `startupTableAsync()` for every table (see TablesLoader).
    /// The returned task is also stored inside the database for cancellation on destruction.
    [[nodiscard]] virtual LoadTaskPtr startupDatabaseAsync(
        AsyncLoader & /*async_loader*/,
        LoadJobSet /*startup_after*/,
        LoadingStrictnessLevel /*mode*/);

    /// Waits for specific table to be started up, i.e. task returned by `startupTableAsync()` is done
    virtual void waitTableStarted(const String & /*name*/) const {}

    /// Waits for the database to be started up, i.e. task returned by `startupDatabaseAsync()` is done
    virtual void waitDatabaseStarted() const {}

    /// Cancels all load and startup tasks and waits for currently running tasks to finish.
    /// Should be used during shutdown to (1) prevent race with startup, (2) stop any not yet started task and (3) avoid exceptions if startup failed
    virtual void stopLoading() {}

    /// Check the existence of the table in memory (attached).
    virtual bool isTableExist(const String & name, ContextPtr context) const = 0;

    /// Check the existence of the table in any state (in active / detached / detached permanently state).
    /// Throws exception when table exists.
    virtual void checkMetadataFilenameAvailability(const String & /*table_name*/) const {}

    /// Check if the table name exceeds the max allowed length
    virtual void checkTableNameLength(const String & /*table_name*/) const {}

    /// Get the table for work. Return nullptr if there is no table.
    virtual StoragePtr tryGetTable(const String & name, ContextPtr context) const = 0;

    virtual StoragePtr getTable(const String & name, ContextPtr context) const;

    virtual UUID tryGetTableUUID(const String & /*table_name*/) const { return UUIDHelpers::Nil; }

    using FilterByNameFunction = std::function<bool(const String &)>;

    /// Get an iterator that allows you to pass through all the tables.
    /// It is possible to have "hidden" tables that are not visible when passing through, but are visible if you get them by name using the functions above.
    /// Wait for all tables to be loaded and started up. If `skip_not_loaded` is true, then not yet loaded or not yet started up (at the moment of iterator creation) tables are excluded.
    virtual DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name = {}, bool skip_not_loaded = false) const = 0; /// NOLINT

    /// Same as above, but may return non-fully initialized StoragePtr objects which are not suitable for reading.
    /// Useful for queries like "SHOW TABLES"
    virtual DatabaseTablesIteratorPtr getLightweightTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name = {}, bool skip_not_loaded = false) const /// NOLINT
    {
        return getTablesIterator(context, filter_by_table_name, skip_not_loaded);
    }

    virtual DatabaseDetachedTablesSnapshotIteratorPtr getDetachedTablesIterator(
        ContextPtr /*context*/, const FilterByNameFunction & /*filter_by_table_name = {}*/, bool /*skip_not_loaded = false*/) const;

    /// Returns list of table names.
    virtual Strings getAllTableNames(ContextPtr context) const
    {
        // NOTE: This default implementation wait for all tables to be loaded and started up. It should be reimplemented for databases that support async loading.
        Strings result;
        for (auto table_it = getTablesIterator(context); table_it->isValid(); table_it->next())
            result.emplace_back(table_it->name());
        return result;
    }

    /// Is the database empty.
    virtual bool empty() const = 0;

    virtual bool isReadOnly() const { return false; }

    /// Add the table to the database. Record its presence in the metadata.
    virtual void createTable(
        ContextPtr /*context*/,
        const String & /*name*/,
        const StoragePtr & /*table*/,
        const ASTPtr & /*query*/);

    /// Delete the table from the database, drop table and delete the metadata.
    virtual void dropTable( /// NOLINT
        ContextPtr /*context*/,
        const String & /*name*/,
        [[maybe_unused]] bool sync = false);

    /// Add a table to the database, but do not add it to the metadata. The database may not support this method.
    ///
    /// Note: ATTACH TABLE statement actually uses createTable method.
    virtual void attachTable(ContextPtr /* context */, const String & /*name*/, const StoragePtr & /*table*/, [[maybe_unused]] const String & relative_table_path);

    /// Forget about the table without deleting it, and return it. The database may not support this method.
    virtual StoragePtr detachTable(ContextPtr /* context */, const String & /*name*/);

    /// Forget about the table without deleting it's data, but rename metadata file to prevent reloading it
    /// with next restart. The database may not support this method.
    virtual void detachTablePermanently(ContextPtr /*context*/, const String & /*name*/);

    /// Returns list of table names that were permanently detached.
    /// This list may not be updated in runtime and may be filled only on server startup
    virtual Strings getNamesOfPermanentlyDetachedTables() const;

    /// Rename the table and possibly move the table to another database.
    virtual void renameTable(
        ContextPtr /*context*/,
        const String & /*name*/,
        IDatabase & /*to_database*/,
        const String & /*to_name*/,
        bool /*exchange*/,
        bool /*dictionary*/);

    using ASTModifier = std::function<void(IAST &)>;

    /// Change the table structure in metadata.
    /// You must call under the alter_lock of the corresponding table . If engine_modifier is empty, then engine does not change.
    virtual void alterTable(
        ContextPtr /*context*/,
        const StorageID & /*table_id*/,
        const StorageInMemoryMetadata & /*metadata*/);

    /// Special method for ReplicatedMergeTree and DatabaseReplicated
    virtual bool canExecuteReplicatedMetadataAlter() const { return true; }

    /// Returns time of table's metadata change, 0 if there is no corresponding metadata file.
    virtual time_t getObjectMetadataModificationTime(const String & /*name*/) const
    {
        return static_cast<time_t>(0);
    }

    /// Get the CREATE TABLE query for the table. It can also provide information for detached tables for which there is metadata.
    ASTPtr tryGetCreateTableQuery(const String & name, ContextPtr context) const noexcept
    {
        return getCreateTableQueryImpl(name, context, false);
    }

    ASTPtr getCreateTableQuery(const String & name, ContextPtr context) const
    {
        return getCreateTableQueryImpl(name, context, true);
    }

    /// Get the CREATE DATABASE query for current database.
    virtual ASTPtr getCreateDatabaseQuery() const = 0;

    String getDatabaseComment() const
    {
        std::lock_guard lock{mutex};
        return comment;
    }
    void setDatabaseComment(String new_comment)
    {
        std::lock_guard lock{mutex};
        comment = std::move(new_comment);
    }

    /// Get name of database.
    String getDatabaseName() const
    {
        std::lock_guard lock{mutex};
        return database_name;
    }

    // Alter comment of database.
    virtual void alterDatabaseComment(const AlterCommand &);

    /// Get UUID of database.
    virtual UUID getUUID() const { return UUIDHelpers::Nil; }

    virtual void renameDatabase(ContextPtr, const String & /*new_name*/);

    /// Returns path for persistent data storage if the database supports it, empty string otherwise
    virtual String getDataPath() const { return {}; }

    /// Returns path for persistent data storage for table if the database supports it, empty string otherwise. Table must exist
    virtual String getTableDataPath(const String & /*table_name*/) const { return {}; }
    /// Returns path for persistent data storage for CREATE/ATTACH query if the database supports it, empty string otherwise
    virtual String getTableDataPath(const ASTCreateQuery & /*query*/) const { return {}; }
    /// Returns metadata path if the database supports it, empty string otherwise
    virtual String getMetadataPath() const { return {}; }
    /// Returns metadata path of a concrete table if the database supports it, empty string otherwise
    virtual String getObjectMetadataPath(const String & /*table_name*/) const { return {}; }

    /// All tables and dictionaries should be detached before detaching the database.
    virtual bool shouldBeEmptyOnDetach() const { return true; }

    virtual void assertCanBeDetached(bool /*cleanup*/) {}

    virtual void waitDetachedTableNotInUse(const UUID & /*uuid*/) { }
    virtual void checkDetachedTableNotInUse(const UUID & /*uuid*/) { }

    /// Ask all tables to complete the background threads they are using and delete all table objects.
    virtual void shutdown() = 0;

    /// Delete data and metadata stored inside the database, if exists.
    virtual void drop(ContextPtr /*context*/) {}

    virtual void applySettingsChanges(const SettingsChanges &, ContextPtr);

    virtual bool hasReplicationThread() const { return false; }

    virtual void stopReplication();

    virtual bool shouldReplicateQuery(const ContextPtr & /*query_context*/, const ASTPtr & /*query_ptr*/) const { return false; }

    virtual BlockIO tryEnqueueReplicatedDDL(const ASTPtr & /*query*/, ContextPtr /*query_context*/, [[maybe_unused]] QueryFlags flags);

    /// Returns CREATE TABLE queries and corresponding tables prepared for writing to a backup.
    virtual std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & context) const;

    /// Creates a table restored from backup.
    virtual void createTableRestoredFromBackup(const ASTPtr & create_table_query, ContextMutablePtr context, std::shared_ptr<IRestoreCoordination> restore_coordination, UInt64 timeout_ms);

    /// Get the disk storing metedata files of the tables
    virtual DiskPtr getDisk() const;

    virtual ~IDatabase();

protected:
    virtual ASTPtr getCreateTableQueryImpl(const String & /*name*/, ContextPtr /*context*/, bool throw_on_error) const;

    mutable std::mutex mutex;
    String database_name TSA_GUARDED_BY(mutex);
    String comment TSA_GUARDED_BY(mutex);
};

using DatabasePtr = std::shared_ptr<IDatabase>;
using ConstDatabasePtr = std::shared_ptr<const IDatabase>;
using Databases = std::map<String, DatabasePtr>;

}
