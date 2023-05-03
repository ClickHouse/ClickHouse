#pragma once
#include <memory>
#include <Databases/DatabasesCommon.h>
#include <Common/escapeForFileName.h>
#include <Common/FoundationDB/MetadataStoreFoundationDB.h>
#include <MetadataTable.pb.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class DatabaseOnFDB final : public DatabaseWithOwnTablesBase
{
public:
    using MetadataTable = FoundationDB::Proto::MetadataTable;
    using IteratingFunction = std::function<void(const String &)>;
    using DatabaseMeta = FoundationDB::Proto::MetadataDatabase;
    using IteratingFDBFunction = std::function<void(const MetadataTable &)>;
    using DetachedTables = std::unordered_map<UUID, StoragePtr>;
    using NameToPathMap = std::unordered_map<String, String>;
    using FDB = std::shared_ptr<MetadataStoreFoundationDB>;
    using TableKey = FoundationDB::TableKey;

    DatabaseOnFDB(String name_, UUID uuid, const String & logger_name, ContextPtr context_);
    DatabaseOnFDB(String name_, UUID uuid, ContextPtr context_);

    String getEngineName() const override { return "OnFDB"; }
    UUID getUUID() const override { return db_uuid; }
    ASTPtr getCreateDatabaseQuery() const override;

    void renameDatabase(ContextPtr query_context, const String & new_name) override;

    bool supportsLoadingInTopologicalOrder() const override { return true; }
    void checkMetadataFilenameAvailability(const String & to_table_name) const override;
    void checkMetadataFilenameAvailabilityUnlocked(const String & to_table_name, std::unique_lock<std::mutex> &) const;
    time_t getObjectMetadataModificationTime(const String & object_name) const override;

    void createTable(ContextPtr context, const String & table_name, const StoragePtr & table, const ASTPtr & query) override;

    void renameTable(
        ContextPtr context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        bool exchange,
        bool dictionary) override;

    void dropTable(ContextPtr context, const String & table_name, bool no_delay) override;

    void alterTable(ContextPtr context, const StorageID & table_id, const StorageInMemoryMetadata & metadata) override;

    void attachTable(ContextPtr context, const String & name, const StoragePtr & table, const String & relative_table_path) override;
    StoragePtr detachTable(ContextPtr context, const String & name) override;
    void detachTablePermanently(ContextPtr context, const String & table_name) override;

    String getDataPath() const override { return data_path; }
    String getTableDataPath(const ASTCreateQuery & query) const override;
    String getMetadataPath() const override { return getContext()->getPath() + "/" + data_path + DatabaseCatalog::getPathForUUID(db_uuid); }

    void iterateMetadataFiles(ContextPtr context, const IteratingFunction & process_metadata_file) const;

    void iterateMetadataFromFDB(const IteratingFDBFunction & process_metadata_from_fdb) const;

    void drop(ContextPtr /*context*/) override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name) const override;

    void loadStoredObjects(ContextMutablePtr context, bool force_restore, bool force_attach, bool skip_startup_tables) override;

    void startupTables(ThreadPool & thread_pool, bool force_restore, bool force_attach) override;

    void loadTablesMetadata(ContextPtr context, ParsedTablesMetadata & metadata) override;

    void loadTableFromMetadata(
        ContextMutablePtr local_context,
        const String & file_path,
        const QualifiedTableName & name,
        const ASTPtr & ast,
        bool force_restore) override;

    void assertCanBeDetached(bool cleanup) override;

    UUID tryGetTableUUID(const String & table_name) const override;

    void waitDetachedTableNotInUse(const UUID & uuid) override;
    void checkDetachedTableNotInUse(const UUID & uuid) override;
    void setDetachedTableNotInUseForce(const UUID & uuid);

    static std::unique_ptr<DatabaseMeta> getDBMetaFromQuery(const ASTCreateQuery & create);
    static ASTPtr getAttachQueryFromDBMeta(ContextPtr context, const DatabaseMeta & meta);
    static std::shared_ptr<MetadataTable> getTableMetaFromQuery(const ASTCreateQuery & create);
    static ASTPtr getQueryFromTableMeta(ContextPtr context, const MetadataTable & tb_meta, bool throw_on_error = true);

protected:
    void removeDetachedPermanentlyFlag(const String & table_name, const String & db_name) const;

    [[nodiscard]] DetachedTables cleanupDetachedTables();
    void assertDetachedTableNotInUse(const UUID & uuid);

    ASTPtr getCreateTableQueryImpl(const String & table_name, ContextPtr context, bool throw_on_error) const override;

    ASTPtr getCreateQueryFromStorage(const String & table_name, const StoragePtr & storage, bool throw_on_error) const;

    const String data_path;
    DetachedTables detached_tables;

    const UUID db_uuid;

    FDB meta_store;
};
}
