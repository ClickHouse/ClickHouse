#pragma once

#include <Databases/DatabaseMetadataDiskSettings.h>
#include <Databases/DatabaseOnDisk.h>
#include <Common/ThreadPool.h>


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

    Strings getAllTableNames(ContextPtr context) const override;

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

    static void setMergeTreeEngine(ASTCreateQuery & create_query, ContextPtr context, bool replicated);

protected:
    virtual void commitAlterTable(
        const StorageID & table_id,
        const String & table_metadata_tmp_path,
        const String & table_metadata_path,
        const String & statement,
        ContextPtr query_context);

    Strings permanently_detached_tables TSA_GUARDED_BY(mutex);

    std::unordered_map<String, LoadTaskPtr> load_table TSA_GUARDED_BY(mutex);
    std::unordered_map<String, LoadTaskPtr> startup_table TSA_GUARDED_BY(mutex);
    LoadTaskPtr startup_database_task TSA_GUARDED_BY(mutex);
    std::atomic<size_t> total_tables_to_startup{0};
    std::atomic<size_t> tables_started{0};
    AtomicStopwatch startup_watch;

    DatabaseMetadataDiskSettings database_metadata_disk_settings;
    DiskPtr metadata_disk_ptr;

private:
    bool shouldLazyLoad(const ASTCreateQuery & query, LoadingStrictnessLevel mode) const;
    void loadTableLazy(
        ContextMutablePtr local_context,
        const QualifiedTableName & name,
        const ASTPtr & ast,
        LoadingStrictnessLevel mode);

    void convertMergeTreeToReplicatedIfNeeded(ASTPtr ast, const QualifiedTableName & qualified_name, const String & file_name);
    void restoreMetadataAfterConvertingToReplicated(StoragePtr table, const QualifiedTableName & name);
    String getConvertToReplicatedFlagPath(const String & name, bool tableStarted);
};

}
