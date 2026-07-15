#pragma once
#include "config.h"

#if USE_AVRO && USE_PARQUET

#include <Databases/DataLake/DataLakeConstants.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/DataLake/DatabaseDataLakeSettings.h>
#include <Databases/DataLake/ICatalog.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Poco/Net/HTTPBasicCredentials.h>

namespace DB
{

class DatabaseDataLake final : public IDatabase, WithContext
{
public:
    explicit DatabaseDataLake(
        const std::string & database_name_,
        const std::string & url_,
        const DatabaseDataLakeSettings & settings_,
        ASTPtr database_engine_definition_,
        ASTPtr table_engine_definition_,
        UUID uuid,
        bool allow_server_credentials_in_user_queries_,
        bool is_loading_from_existing_metadata_,
        bool lazy_init);

    String getEngineName() const override { return DataLake::DATABASE_ENGINE_NAME; }
    UUID getUUID() const override { return db_uuid; }

    bool shouldBeEmptyOnDetach() const override { return false; }
    bool isDatalakeCatalog() const override { return true; }

    bool empty() const override;

    bool isTableExist(const String & name, ContextPtr context) const override;
    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;

    /// skip_not_loaded flag ignores all non-iceberg tables
    DatabaseTablesIteratorPtr getTablesIterator(
        ContextPtr context,
        const FilterByNameFunction & filter_by_table_name,
        bool skip_not_loaded) const override;

    DatabaseTablesIteratorPtr getTablesIteratorWithHint(
        ContextPtr context,
        const FilterByNameFunction & filter_by_table_name,
        bool skip_not_loaded,
        const TablesFilter & tables_filter) const override;

    /// skip_not_loaded flag ignores all non-iceberg tables
    std::vector<LightWeightTableDetails> getLightweightTablesIterator(
        ContextPtr context,
        const FilterByNameFunction & filter_by_table_name,
        bool skip_not_loaded) const override;

    std::vector<LightWeightTableDetails> getLightweightTablesIteratorWithHint(
        ContextPtr context,
        const FilterByNameFunction & filter_by_table_name,
        bool skip_not_loaded,
        const TablesFilter & tables_filter) const override;

    VectorWithMemoryTracking<String> getAllTableNames(ContextPtr context) const override;

    void checkDatabase() const override;

    void shutdown() override {}

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const override { return {}; }

    void createTable(
        ContextPtr /*context*/,
        const String & /*name*/,
        const StoragePtr & /*table*/,
        const ASTPtr & /*query*/) override {}

    void dropTable( /// NOLINT
        ContextPtr context_,
        const String & name,
        bool /*sync*/) override;

    std::shared_ptr<DataLake::ICatalog> getCatalog() const;
protected:
    ASTPtr getCreateDatabaseQueryImpl() const override TSA_REQUIRES(mutex);
    ASTPtr getCreateTableQueryImpl(const String & table_name, ContextPtr context, bool throw_on_error) const override;

private:
    /// Iceberg Catalog url.
    const std::string url;
    /// SETTINGS from CREATE query.
    const DatabaseDataLakeSettings settings;
    /// Database engine definition taken from initial CREATE DATABASE query.
    const ASTPtr database_engine_definition;
    const ASTPtr table_engine_definition;
    const LoggerPtr log;
    /// Crendetials to authenticate Iceberg Catalog.
    Poco::Net::HTTPBasicCredentials credentials;
    /// Effective `s3_allow_server_credentials_in_user_queries` captured when the database was created (or
    /// implied when it is loaded from existing metadata). The catalog clients are built once and cached, so
    /// the restriction cannot be read from the query context of whichever query touches the catalog first.
    const bool allow_server_credentials_in_user_queries;
    /// True when the database is loaded from existing metadata (server startup or RESTORE). If the catalog
    /// then fails to authenticate because its credentials are server-managed and restricted, the catalog is
    /// left unavailable (rather than aborting startup), so the server still starts and only this database is
    /// inaccessible -- mirroring the behavior of persistent S3/S3Queue tables.
    const bool is_loading_from_existing_metadata;

    mutable std::mutex catalog_mutex;
    mutable std::shared_ptr<DataLake::ICatalog> catalog_impl TSA_GUARDED_BY(catalog_mutex);
    /// Set when `catalog_impl` could not be built because its server-managed credentials are restricted on
    /// load; `getCatalog` then throws this so every query against the database reports a clear error.
    mutable String catalog_unavailable_reason TSA_GUARDED_BY(catalog_mutex);

    void validateSettings();

    /// Builds `catalog_impl` based on the configured catalog type. Constructing a catalog can
    /// validate credentials and perform network I/O (e.g. RestCatalog reads the catalog config),
    /// so on ATTACH (server startup) it is deferred to the first access via `getCatalog` instead
    /// of running eagerly in the constructor. That keeps one misconfigured or unreachable database
    /// from blocking server startup. On CREATE it still runs eagerly so problems are reported up
    /// front. Guarded by `catalog_mutex` because lazy initialization can race concurrent readers.
    void initialize() const TSA_REQUIRES(catalog_mutex);

    /// `initialize`, but when loading from existing metadata a catalog that resolves the now-restricted server
    /// identity is left unavailable (its reason recorded) instead of propagating, so server startup is not
    /// aborted; a user-initiated create/attach stays fail-closed and the `ACCESS_DENIED` propagates.
    void initializeOrLeaveUnavailable() const TSA_REQUIRES(catalog_mutex);

    std::shared_ptr<StorageObjectStorageConfiguration> getConfiguration(
        DatabaseDataLakeStorageType type,
        DataLakeStorageSettingsPtr storage_settings) const;

    std::string getStorageEndpointForTable(const DataLake::TableMetadata & table_metadata) const;

    /// Shared implementation of getTablesIterator / getTablesIteratorWithHint.
    /// keep_unresolved_tables controls what happens when a single table's metadata cannot
    /// be resolved: when true (system.tables path) the table is kept in the listing with a
    /// null storage object so metadata-dependent columns degrade to defaults instead of the
    /// whole scan aborting; when false (every other consumer, e.g. StorageMerge, which
    /// dereferences the storage unconditionally) the original contract is preserved -- the
    /// error is propagated when database_datalake_require_metadata_access=1 and the table is
    /// dropped from the listing otherwise. This confines null-storage rows to system.tables.
    DatabaseTablesIteratorPtr getTablesIteratorImpl(
        ContextPtr context,
        const FilterByNameFunction & filter_by_table_name,
        bool skip_not_loaded,
        const TablesFilter & tables_filter,
        bool keep_unresolved_tables) const;

    /// Can return nullptr in case of *expected* issues with response from catalog. Sometimes
    /// catalogs can produce completely unexpected responses. In such cases this function may throw.
    StoragePtr tryGetTableImpl(const String & name, ContextPtr context, bool lightweight, bool ignore_if_not_iceberg) const;

    const UUID db_uuid;
};

}
#endif
