#pragma once

#include <Databases/DatabaseOnDisk.h>
#include <Parsers/ASTCreateQuery.h>


namespace DB
{


class DatabaseLazyIterator;
class Context;

/** Lazy engine of databases.
  * Works like DatabaseOrdinary, but stores in memory only cache.
  * Can be used only with *Log engines.
  */
class DatabaseLazy final : public DatabaseOnDisk
{
public:
    DatabaseLazy(const String & name_, const String & metadata_path_, time_t expiration_time_, ContextPtr context_);

    String getEngineName() const override { return "Lazy"; }

    bool canContainMergeTreeTables() const override { return false; }

    bool canContainDistributedTables() const override { return false; }

    void loadStoredObjects(ContextMutablePtr context, bool force_restore, bool force_attach, bool skip_startup_tables) override;

    void createTable(
        ContextPtr context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void dropTable(
        ContextPtr context,
        const String & table_name,
        bool no_delay) override;

    void renameTable(
        ContextPtr context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        bool exchange,
        bool dictionary) override;

    void alterTable(
        ContextPtr context,
        const StorageID & table_id,
        const StorageInMemoryMetadata & metadata) override;

    time_t getObjectMetadataModificationTime(const String & table_name) const override;

    bool isTableExist(const String & table_name, ContextPtr) const override { return isTableExist(table_name); }
    bool isTableExist(const String & table_name) const;

    StoragePtr tryGetTable(const String & table_name, ContextPtr) const override { return tryGetTable(table_name); }
    StoragePtr tryGetTable(const String & table_name) const;

    bool empty() const override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name) const override;

    void attachTable(const String & table_name, const StoragePtr & table, const String & relative_table_path) override;

    StoragePtr detachTable(const String & table_name) override;

    void shutdown() override;

    ~DatabaseLazy() override;

private:
    struct CacheExpirationQueueElement
    {
        time_t last_touched;
        String table_name;

        CacheExpirationQueueElement(time_t last_touched_, const String & table_name_)
            : last_touched(last_touched_), table_name(table_name_) {}
    };

    using CacheExpirationQueue = std::list<CacheExpirationQueueElement>;


    struct CachedTable
    {
        StoragePtr table;
        time_t last_touched;
        time_t metadata_modification_time;
        CacheExpirationQueue::iterator expiration_iterator;

        CachedTable() = delete;
        CachedTable(const StoragePtr & table_, time_t last_touched_, time_t metadata_modification_time_)
            : table(table_), last_touched(last_touched_), metadata_modification_time(metadata_modification_time_) {}
    };

    using TablesCache = std::unordered_map<String, CachedTable>;

    const time_t expiration_time;

    /// TODO use DatabaseWithOwnTablesBase::tables
    mutable TablesCache tables_cache;
    mutable CacheExpirationQueue cache_expiration_queue;

    StoragePtr loadTable(const String & table_name) const;

    void clearExpiredTables() const;

    friend class DatabaseLazyIterator;
};


class DatabaseLazyIterator final : public IDatabaseTablesIterator
{
public:
    DatabaseLazyIterator(
        const DatabaseLazy & database_,
        Strings && table_names_);

    void next() override;
    bool isValid() const override;
    const String & name() const override;
    const StoragePtr & table() const override;

private:
    const DatabaseLazy & database;
    const Strings table_names;
    Strings::const_iterator iterator;
    mutable StoragePtr current_storage;
};
}
