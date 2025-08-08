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
        ASTPtr table_engine_definition_);

    String getEngineName() const override { return DataLake::DATABASE_ENGINE_NAME; }

    bool canContainMergeTreeTables() const override { return false; }
    bool canContainDistributedTables() const override { return false; }
    bool canContainRocksDBTables() const override { return false; }
    bool shouldBeEmptyOnDetach() const override { return false; }

    bool empty() const override;

    bool isTableExist(const String & name, ContextPtr context) const override;
    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;

    /// skip_not_loaded flag ignores all non-iceberg tables
    DatabaseTablesIteratorPtr getTablesIterator(
        ContextPtr context,
        const FilterByNameFunction & filter_by_table_name,
        bool skip_not_loaded) const override;

    /// skip_not_loaded flag ignores all non-iceberg tables
    DatabaseTablesIteratorPtr getLightweightTablesIterator(
        ContextPtr context,
        const FilterByNameFunction & filter_by_table_name,
        bool skip_not_loaded) const override;


    void shutdown() override {}

    ASTPtr getCreateDatabaseQuery() const override;

protected:
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

    mutable std::shared_ptr<DataLake::ICatalog> catalog_impl;

    void validateSettings();
    std::shared_ptr<DataLake::ICatalog> getCatalog() const;

    std::shared_ptr<StorageObjectStorage::Configuration> getConfiguration(
        DatabaseDataLakeStorageType type,
        DataLakeStorageSettingsPtr storage_settings) const;

    std::string getStorageEndpointForTable(const DataLake::TableMetadata & table_metadata) const;

    /// Can return nullptr in case of *expected* issues with response from catalog. Sometimes
    /// catalogs can produce completely unexpected responses. In such cases this function may throw.
    StoragePtr tryGetTableImpl(const String & name, ContextPtr context, bool lightweight, bool ignore_if_not_iceberg) const;
};

}
#endif
