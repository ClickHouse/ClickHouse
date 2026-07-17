#pragma once

#include "config.h"

#if USE_PARQUET

#include <Core/Types.h>
#include <Formats/FormatFilterInfo.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>

namespace DB
{

class DuckLakeCatalog;

/// DuckLake table metadata pinned to one catalog snapshot_id.
/// Immutable per query (supportsUpdate() == false): every query re-creates it and re-pins.
class DuckLakeMetadata final : public IDataLakeMetadata
{
public:
    static constexpr auto name = "DuckLake";
    const char * getName() const override { return name; }

    DuckLakeMetadata(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationWeakPtr configuration_,
        std::shared_ptr<DuckLakeCatalog> catalog_,
        Int64 snapshot_id_,
        Int64 table_id_,
        NamesAndTypesList schema_,
        ColumnMapperPtr column_mapper_,
        String catalog_table_path_,
        String storage_table_path_);

    static DataLakeMetadataPtr create(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationWeakPtr & configuration,
        const ContextPtr & local_context);

    static void createInitial(
        const ObjectStoragePtr & /*object_storage*/,
        const StorageObjectStorageConfigurationWeakPtr & /*configuration*/,
        const ContextPtr & /*local_context*/,
        const std::optional<ColumnsDescription> & /*columns*/,
        ASTPtr /*partition_by*/,
        ASTPtr /*order_by*/,
        bool /*if_not_exists*/,
        std::shared_ptr<DataLake::ICatalog> /*catalog*/,
        const StorageID & /*table_id*/);

    NamesAndTypesList getTableSchema(ContextPtr /*local_context*/) const override { return schema; }

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size,
        StorageMetadataPtr storage_metadata_snapshot,
        ContextPtr context) const override;

    bool operator==(const IDataLakeMetadata & other) const override;

    ColumnMapperPtr getColumnMapperForCurrentSchema(StorageMetadataPtr, ContextPtr) const override { return column_mapper; }
    ColumnMapperPtr getColumnMapperForObject(ObjectInfoPtr) const override { return column_mapper; }

    void modifyFormatSettings(FormatSettings & format_settings, const Context &) const override;

    void addDeleteTransformers(
        ObjectInfoPtr object_info,
        QueryPipelineBuilder & builder,
        const std::optional<FormatSettings> & format_settings,
        FormatParserSharedResourcesPtr parser_shared_resources,
        ContextPtr context) const override;

private:
    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationWeakPtr configuration;
    std::shared_ptr<DuckLakeCatalog> catalog;
    Int64 snapshot_id;
    Int64 table_id;
    NamesAndTypesList schema;
    ColumnMapperPtr column_mapper;
    /// Catalog-side table data path (scheme stripped, no trailing slash), used to verify
    /// absolute file paths from the catalog belong to this table.
    String catalog_table_path;
    /// Storage-side table path (the configuration's raw path, no trailing slash): a filesystem
    /// path for local storage or a key prefix for remote storage. Object paths are composed
    /// as storage_table_path + '/' + <catalog-relative path>.
    String storage_table_path;

    String toObjectPath(const String & path, bool path_is_relative) const;
};

}

#endif
