#pragma once

#include <Databases/DatabaseMetadataDiskSettings.h>
#include <Databases/DatabaseOnDisk.h>
#include <Storages/TableZnodeInfo.h>


namespace DB
{

/** Default engine of databases.
  * It stores tables list in filesystem using list of .sql files,
  *  that contain declaration of table represented by SQL ATTACH TABLE query.
  */
class DatabaseOrdinary : public DatabaseOnDisk
{
public:
    DatabaseOrdinary(
        const String & name_,
        const String & metadata_path_,
        ContextPtr context,
        DatabaseMetadataDiskSettings database_metadata_disk_settings_ = {});
    DatabaseOrdinary(
        const String & name_,
        const String & metadata_path_,
        const String & data_path_,
        const String & logger,
        ContextPtr context_,
        DatabaseMetadataDiskSettings database_metadata_disk_settings_ = {});

    String getEngineName() const override { return "Ordinary"; }

    void loadStoredObjects(ContextMutablePtr context, LoadingStrictnessLevel mode) override;

    bool supportsLoadingInTopologicalOrder() const override { return true; }

    void loadTablesMetadata(ContextPtr context, ParsedTablesMetadata & metadata, bool is_startup) override;

    void loadTableFromMetadata(
        ContextMutablePtr local_context,
        const String & file_path,
        const QualifiedTableName & name,
        const ASTPtr & ast,
        LoadingStrictnessLevel mode) override;

    LoadTaskPtr loadTableFromMetadataAsync(
        AsyncLoader & async_loader,
        LoadJobSet load_after,
        ContextMutablePtr local_context,
        const String & file_path,
        const QualifiedTableName & name,
        const ASTPtr & ast,
        LoadingStrictnessLevel mode) override;

    LoadTaskPtr startupTableAsync(
        AsyncLoader & async_loader,
        LoadJobSet startup_after,
        const QualifiedTableName & name,
        LoadingStrictnessLevel mode) override;

    void waitTableStarted(const String & name) const override;

    void waitDatabaseStarted() const override;
    void stopLoading() override;

    LoadTaskPtr startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode) override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr local_context, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;
    DatabaseDetachedTablesSnapshotIteratorPtr getDetachedTablesIterator(
        ContextPtr local_context, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    VectorWithMemoryTracking<String> getAllTableNames(ContextPtr context) const override;

    StoragePtr detachTableUnlocked(const String & table_name) TSA_REQUIRES(mutex) override;

    /// Remembers the detached storage for the liveness guard below.
    StoragePtr detachTable(ContextPtr context, const String & name) override;

    /// The detached-table liveness guard for tables without a UUID, keyed by table name. `ATTACH ... AS REPLICATED`
    /// deletes transaction metadata files from disk, which must not happen while the previous storage instance of
    /// the same table is still alive: its parts remember those files and check them on destruction.
    /// `DatabaseAtomic` keeps its own UUID-keyed bookkeeping (`checkDetachedTableNotInUse`) and never fills this one.
    void waitDetachedTableByNameNotInUse(const String & table_name, std::function<void()> throw_if_cancelled);
    void checkDetachedTableByNameNotInUse(const String & table_name);

    void alterTable(
        ContextPtr context,
        const StorageID & table_id,
        const StorageInMemoryMetadata & metadata,
        bool validate_new_create_query) override;

    Strings getNamesOfPermanentlyDetachedTables() const override
    {
        std::lock_guard lock(mutex);
        return permanently_detached_tables;
    }

    DiskPtr getDisk() const override { return metadata_disk_ptr; }

    static void setMergeTreeEngine(ASTCreateQuery & create_query, ContextPtr context, bool replicated, bool ordinary_database);

    /// Rejects a conversion to a replicated engine whose Keeper path would not be a safe one.
    /// Contacts nothing and mutates nothing, so a caller can run it before its own side effects.
    /// Returns the resolved path, split into the Keeper cluster name and the raw path inside it.
    /// `stores_path_literally` is set for a table of an `Ordinary` database: its metadata keeps the fully
    /// expanded path instead of the template, so the path must also survive being read back as a literal.
    static TableZnodeInfo checkReplicaPathIsSafe(const ASTCreateQuery & create_query, ContextPtr context, bool stores_path_literally);

protected:
    /// Erase pending async load/startup task references for a table. Must hold `mutex`.
    /// Shared by detachTableUnlocked and the Atomic rename detach path (issue #91777).
    void eraseAsyncLoadState(const String & table_name) TSA_REQUIRES(mutex);

    virtual void commitAlterTable(
        const StorageID & table_id,
        const String & table_metadata_tmp_path,
        const String & table_metadata_path,
        const String & statement,
        ContextPtr query_context);

    Strings permanently_detached_tables TSA_GUARDED_BY(mutex);

    /// Weak references: tracking must never extend the lifetime of a detached storage.
    /// A name may have several live detached instances at once: a plain `ATTACH TABLE` of an `Ordinary` table
    /// (no UUID) does not wait for the previous instance, so `DETACH` -> `ATTACH` -> `DETACH` leaves two of them.
    /// Every instance is kept until it expires, so that none of them is forgotten while its parts are alive.
    std::unordered_multimap<String, std::weak_ptr<IStorage>> detached_tables_by_name TSA_GUARDED_BY(mutex);
    /// Forgets the storages that are gone or were renamed away. Every strong reference the sweep takes is
    /// handed to `keep_alive`, so that the caller lets it go only after releasing `mutex`: had the sweep been the
    /// last owner of a storage, the storage would otherwise be destroyed under the database mutex, which
    /// `DatabaseAtomic::cleanupDetachedTables` avoids for the same reason (that destruction can deadlock).
    void forgetExpiredDetachedTablesByName(std::vector<StoragePtr> & keep_alive) TSA_REQUIRES(mutex);
    /// Forgets the storages that are gone or were renamed away, then tells whether `table_name` is still in use.
    /// `keep_alive` has the same contract as above.
    bool isDetachedTableByNameInUse(const String & table_name, std::vector<StoragePtr> & keep_alive) TSA_REQUIRES(mutex);

    std::unordered_map<String, LoadTaskPtr> load_table TSA_GUARDED_BY(mutex);
    std::unordered_map<String, LoadTaskPtr> startup_table TSA_GUARDED_BY(mutex);
    LoadTaskPtr startup_database_task TSA_GUARDED_BY(mutex);
    std::atomic<size_t> total_tables_to_startup{0};
    std::atomic<size_t> tables_started{0};
    AtomicStopwatch startup_watch;

    DatabaseMetadataDiskSettings database_metadata_disk_settings;
    DiskPtr metadata_disk_ptr;

private:
    bool shouldLazyLoad(const ASTCreateQuery & query, const QualifiedTableName & name, LoadingStrictnessLevel mode) const;
    void loadTableLazy(
        ContextMutablePtr local_context,
        const QualifiedTableName & name,
        const ASTPtr & ast,
        LoadingStrictnessLevel mode);

    void convertMergeTreeToReplicatedIfNeeded(ASTPtr ast, const QualifiedTableName & qualified_name, const String & file_name);
    void restoreMetadataAfterConvertingToReplicated(StoragePtr table, const QualifiedTableName & name);
    /// The flag lives in the table's data directory. Take the create query for a table that is not
    /// attached yet: the data path then comes from the UUID in the query, whereas the name overload
    /// resolves the path through the database's attached-table map.
    String getConvertToReplicatedFlagPath(const ASTCreateQuery & create_query);
    String getConvertToReplicatedFlagPath(const String & table_name);
};

}
