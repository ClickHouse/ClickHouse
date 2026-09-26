#pragma once

#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <base/types.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_set>


/// General functionality for several different database engines.

namespace DB
{

class IDisk;

void applyMetadataChangesToCreateQuery(const ASTPtr & query, const StorageInMemoryMetadata & metadata, ContextPtr context, bool validate_new_create_query = true);

/// Throws QUERY_IS_TOO_LARGE if the resulting CREATE query for `metadata` exceeds max_query_size.
/// `table_id` is used to fetch the current CREATE query AST and for the error message.
void checkMetadataDoesNotExceedMaxQuerySize(const StorageID & table_id, const StorageInMemoryMetadata & metadata, ContextPtr context);
ASTPtr getCreateQueryFromStorage(const StoragePtr & storage, const ASTPtr & ast_storage, bool only_ordinary,
    uint32_t max_parser_depth, uint32_t max_parser_backtracks, bool throw_on_error, ContextPtr context);

/// Cleans a CREATE QUERY from temporary flags like "IF NOT EXISTS", "OR REPLACE", "AS SELECT" (for non-views), etc.
void cleanupObjectDefinitionFromTemporaryFlags(ASTCreateQuery & query);

String readMetadataFile(std::shared_ptr<IDisk> disk, const String & file_path);
void writeMetadataFile(std::shared_ptr<IDisk> disk, const String & file_path, std::string_view content, bool fsync_metadata);

/// TODO: move more common code to here
class DatabaseWithAltersOnDiskBase : public IDatabase
{
    using IDatabase::IDatabase;

public:
    void alterDatabaseComment(const AlterCommand & command, ContextPtr query_context) override;
};

/// A base class for databases that manage their own list of tables.
class DatabaseWithOwnTablesBase : public DatabaseWithAltersOnDiskBase, protected WithContext
{
public:
    bool isExternal() const override { return false; }

    bool isTableExist(const String & table_name, ContextPtr context) const override;

    StoragePtr tryGetTable(const String & table_name, ContextPtr context) const override;

    bool empty() const override;

    void attachTable(ContextPtr context, const String & table_name, const StoragePtr & table, const String & relative_table_path) override;

    StoragePtr detachTable(ContextPtr context, const String & table_name) override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    DatabaseDetachedTablesSnapshotIteratorPtr
    getDetachedTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const override;
    void createTableRestoredFromBackup(const ASTPtr & create_table_query, ContextMutablePtr local_context, std::shared_ptr<IRestoreCoordination> restore_coordination, UInt64 timeout_ms) override;

    void shutdown() override;

    ~DatabaseWithOwnTablesBase() override;

    void setDeferredPopulation(std::function<void(IDatabase &)> populate);

    void ensurePopulated() const TSA_NO_THREAD_SAFETY_ANALYSIS;

protected:
    bool mayShadowDeferredTable(const String & table_name) const;

    /// Mutable because a lazily loaded table replaces itself with the storage it stands for as soon
    /// as the load is noticed, which happens on the (const) lookup paths.
    mutable Tables tables TSA_GUARDED_BY(mutex);
    SnapshotDetachedTables snapshot_detached_tables TSA_GUARDED_BY(mutex);
    LoggerPtr log;

    DatabaseWithOwnTablesBase(const String & name_, const String & logger, ContextPtr context);

    void attachTableUnlocked(const String & table_name, const StoragePtr & table) TSA_REQUIRES(mutex);
    virtual StoragePtr detachTableUnlocked(const String & table_name) TSA_REQUIRES(mutex);
    StoragePtr getTableUnlocked(const String & table_name) const TSA_REQUIRES(mutex);
    StoragePtr tryGetTableNoWait(const String & table_name) const;

    /// If `it` holds a lazily loaded table (see the `lazy_load_tables` database setting) that has
    /// already been loaded, put the storage it stands for in its place and return that storage;
    /// otherwise return what is already there.
    ///
    /// Doing this makes everything that resolves the table from now on - including everything that
    /// only recognizes a table by its concrete type, such as `system.parts` or mutations - see the
    /// real storage instead of the proxy. It has to happen here rather than in the proxy itself,
    /// because the proxy loads the table from under this mutex (`DatabaseAtomic::renameTable` does,
    /// through `tryCreateSymlink`), so it cannot take it.
    StoragePtr replaceLoadedLazyTableUnlocked(Tables::iterator it) const TSA_REQUIRES(mutex);

private:
    mutable std::atomic<bool> has_deferred_population{false};
    mutable std::mutex populate_mutex;
    mutable std::condition_variable populated;
    mutable std::function<void(IDatabase &)> deferred_populate TSA_GUARDED_BY(populate_mutex);
    mutable bool populating TSA_GUARDED_BY(populate_mutex) = false;
    mutable std::thread::id populating_thread TSA_GUARDED_BY(populate_mutex);
    mutable std::exception_ptr deferred_populate_error TSA_GUARDED_BY(populate_mutex);
    mutable std::atomic<bool> deferred_populate_failed{false};
    std::unordered_set<String> deferred_shadowing_candidates;
};

}
