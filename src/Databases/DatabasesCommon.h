#pragma once

#include <base/types.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage_fwd.h>
#include <Databases/IDatabase.h>
#include <mutex>


/// General functionality for several different database engines.

namespace DB
{

void applyMetadataChangesToCreateQuery(const ASTPtr & query, const StorageInMemoryMetadata & metadata);
ASTPtr getCreateQueryFromStorage(const StoragePtr & storage, const ASTPtr & ast_storage, bool only_ordinary, uint32_t max_parser_depth, bool throw_on_error);

/// Cleans a CREATE QUERY from temporary flags like "IF NOT EXISTS", "OR REPLACE", "AS SELECT" (for non-views), etc.
void cleanupObjectDefinitionFromTemporaryFlags(ASTCreateQuery & query);

class Context;

/// A base class for databases that manage their own list of tables.
class DatabaseWithOwnTablesBase : public IDatabase, protected WithContext
{
public:
    bool isTableExist(const String & table_name, ContextPtr context) const override;

    StoragePtr tryGetTable(const String & table_name, ContextPtr context) const override;

    bool empty() const override;

    StoragePtr detachTable(ContextPtr context, const String & table_name) override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name) const override;

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const override;
    void createTableRestoredFromBackup(const ASTPtr & create_table_query, ContextMutablePtr local_context, std::shared_ptr<IRestoreCoordination> restore_coordination, UInt64 timeout_ms) override;

    void shutdown() override;

    ~DatabaseWithOwnTablesBase() override;

protected:
    Tables tables TSA_GUARDED_BY(mutex);
    /// Tables that are attached lazily
    mutable LazyTables lazy_tables TSA_GUARDED_BY(mutex);
    Poco::Logger * log;

    DatabaseWithOwnTablesBase(const String & name_, const String & logger, ContextPtr context);

    void attachTableUnlocked(ContextPtr context, const String & name, const StoragePtr & table, const String & relative_table_path) TSA_REQUIRES(mutex) override;
    void registerLazyTableUnlocked(const String & table_name, LazyTableCreator table_creator, const String & relative_table_path) TSA_REQUIRES(mutex) override;
    StoragePtr detachTableUnlocked(const String & table_name)  TSA_REQUIRES(mutex);
    StoragePtr getTableUnlocked(const String & table_name) const TSA_REQUIRES(mutex);
    StoragePtr tryGetTableNoWait(const String & table_name) const;

    void loadLazyTables() const TSA_REQUIRES(mutex);
};

}
