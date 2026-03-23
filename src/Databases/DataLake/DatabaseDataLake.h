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

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class IcebergMetadata;
class DeltaLakeMetadata;
class PaimonMetadata;
class HudiMetadata;

class DatabaseDataLake final : public IDatabase, WithContext
{
public:
    explicit DatabaseDataLake(
        const std::string & database_name_,
        const std::string & url_,
        const DatabaseDataLakeSettings & settings_,
        ASTPtr database_engine_definition_,
        ASTPtr table_engine_definition_,
        UUID uuid);

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

    /// skip_not_loaded flag ignores all non-iceberg tables
    std::vector<LightWeightTableDetails> getLightweightTablesIterator(
        ContextPtr context,
        const FilterByNameFunction & filter_by_table_name,
        bool skip_not_loaded) const override;


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

    mutable std::shared_ptr<DataLake::ICatalog> catalog_impl;

    void validateSettings();
    std::shared_ptr<DataLake::ICatalog> getCatalog() const;

    static std::shared_ptr<StorageObjectStorageConfiguration> getConfiguration(
        DatabaseDataLakeStorageType type);

    /// Calls `func.template operator()<MetadataType>()` with the correct
    /// metadata type determined by the catalog type.
    template <typename Func>
    auto dispatchByMetadataType(Func && func) const;

    std::string getStorageEndpointForTable(const DataLake::TableMetadata & table_metadata) const;

    /// Can return nullptr in case of *expected* issues with response from catalog. Sometimes
    /// catalogs can produce completely unexpected responses. In such cases this function may throw.
    StoragePtr tryGetTableImpl(const String & name, ContextPtr context, bool lightweight, bool ignore_if_not_iceberg) const;

    const UUID db_uuid;
};

template <typename Func>
auto DatabaseDataLake::dispatchByMetadataType(Func && func) const
{
    auto catalog = getCatalog();
    switch (catalog->getCatalogType())
    {
#if USE_AVRO
        case DatabaseDataLakeCatalogType::ICEBERG_REST:
        case DatabaseDataLakeCatalogType::ICEBERG_HIVE:
        case DatabaseDataLakeCatalogType::ICEBERG_ONELAKE:
        case DatabaseDataLakeCatalogType::ICEBERG_BIGLAKE:
        case DatabaseDataLakeCatalogType::GLUE:
            return func.template operator()<IcebergMetadata>();
        case DatabaseDataLakeCatalogType::PAIMON_REST:
            return func.template operator()<PaimonMetadata>();
#endif
#if USE_PARQUET && USE_DELTA_KERNEL_RS
        case DatabaseDataLakeCatalogType::UNITY:
            return func.template operator()<DeltaLakeMetadata>();
#endif
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Unsupported catalog type for datalake operation");
    }
}

}
#endif
