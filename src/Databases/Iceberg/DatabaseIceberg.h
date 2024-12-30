#pragma once
#include "config.h"

#if USE_AVRO
#include <Databases/DatabasesCommon.h>
#include <Databases/Iceberg/DatabaseIcebergSettings.h>
#include <Databases/Iceberg/ICatalog.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Poco/Net/HTTPBasicCredentials.h>

namespace DB
{

class DatabaseIceberg final : public IDatabase, WithContext
{
public:
    explicit DatabaseIceberg(
        const std::string & database_name_,
        const std::string & url_,
        const DatabaseIcebergSettings & settings_,
        ASTPtr database_engine_definition_);

    String getEngineName() const override { return "Iceberg"; }

    bool canContainMergeTreeTables() const override { return false; }
    bool canContainDistributedTables() const override { return false; }
    bool shouldBeEmptyOnDetach() const override { return false; }

    bool empty() const override;

    bool isTableExist(const String & name, ContextPtr context) const override;
    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;

    DatabaseTablesIteratorPtr getTablesIterator(
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
    const DatabaseIcebergSettings settings;
    /// Database engine definition taken from initial CREATE DATABASE query.
    const ASTPtr database_engine_definition;
    const LoggerPtr log;
    /// Crendetials to authenticate Iceberg Catalog.
    Poco::Net::HTTPBasicCredentials credentials;

    mutable std::shared_ptr<Iceberg::ICatalog> catalog_impl;

    void validateSettings();
    std::shared_ptr<Iceberg::ICatalog> getCatalog() const;
    std::shared_ptr<StorageObjectStorage::Configuration> getConfiguration(DatabaseIcebergStorageType type) const;
    std::string getStorageEndpointForTable(const Iceberg::TableMetadata & table_metadata) const;
};

}
#endif
