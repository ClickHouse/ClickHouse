#pragma once

#include <base/types.h>

#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

#include <Databases/IDatabase.h>
#include <Databases/DatabaseOrdinary.h>

#include <Backups/BackupInfo.h>

namespace DB
{

class Context;
class IBackup;

/// DatabaseBackup provides access to backup tables for specified database.
class DatabaseBackup final : public DatabaseOrdinary
{
public:
    struct Configuration
    {
        std::string database_name;
        BackupInfo backup_info;
    };

    DatabaseBackup(const String & name, const String & metadata_path, const Configuration & config, ContextPtr context);

    String getEngineName() const override { return "Backup"; }

    bool shouldBeEmptyOnDetach() const override { return false; }

    void createTable(
        ContextPtr context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    StoragePtr detachTable(ContextPtr context, const String & name) override;

    void detachTablePermanently(ContextPtr context, const String & table_name) override;

    void dropTable(
        ContextPtr context,
        const String & table_name,
        bool sync) override;

    void renameTable(
        ContextPtr context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        bool exchange,
        bool dictionary) override;

    void alterTable(ContextPtr context, const StorageID & table_id, const StorageInMemoryMetadata & metadata, bool validate_new_create_query) override;

    bool isReadOnly() const override { return true; }

    void beforeLoadingMetadata(ContextMutablePtr local_context, LoadingStrictnessLevel mode) override;

    void loadTablesMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata, bool is_startup) override;

    ASTPtr getCreateQueryFromMetadata(const String & table_name, bool throw_on_error) const override;

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const override;

protected:
    ASTPtr getCreateDatabaseQueryImpl() const override TSA_REQUIRES(mutex);

private:
    const Configuration config;

    std::shared_ptr<IBackup> backup;

    mutable std::mutex table_name_to_create_query_mutex;
    std::unordered_map<String, ASTPtr> table_name_to_create_query TSA_GUARDED_BY(table_name_to_create_query_mutex);
};

}
