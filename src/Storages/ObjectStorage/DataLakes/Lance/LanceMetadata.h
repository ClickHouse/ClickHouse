#pragma once

#include "config.h"

#if USE_LANCE

#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

namespace DB
{

class LanceMetadata final : public IDataLakeMetadata
{
public:
    static constexpr auto name = "Lance";

    explicit LanceMetadata(StorageObjectStorageConfigurationWeakPtr configuration_);

    const char * getName() const override { return name; }

    static DataLakeMetadataPtr create(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationWeakPtr & configuration,
        const ContextPtr & local_context);

    static void createInitial(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationWeakPtr & configuration,
        const ContextPtr & local_context,
        const std::optional<ColumnsDescription> & columns,
        ASTPtr partition_by,
        ASTPtr order_by,
        bool if_not_exists,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const StorageID & table_id_);

    bool operator==(const IDataLakeMetadata & other) const override;

    NamesAndTypesList getTableSchema(ContextPtr local_context) const override;
    std::optional<DataLakeTableStateSnapshot> getTableStateSnapshot(ContextPtr local_context) const override;
    std::unique_ptr<StorageInMemoryMetadata> buildStorageMetadataFromState(
        const DataLakeTableStateSnapshot & state, ContextPtr local_context) const override;
    bool shouldReloadSchemaForConsistency(ContextPtr) const override { return true; }

    ReadFromFormatInfo prepareReadingFromFormat(
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context,
        bool supports_subset_of_columns,
        bool supports_tuple_elements) override;

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size,
        StorageMetadataPtr storage_metadata,
        ContextPtr local_context) const override;

    std::optional<Pipe> read(
        ObjectInfoPtr object_info,
        const ReadFromFormatInfo & read_from_format_info,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr local_context,
        size_t max_block_size,
        FormatParserSharedResourcesPtr parser_shared_resources,
        FormatFilterInfoPtr format_filter_info,
        bool need_only_count,
        std::optional<size_t> limit = {}) const override;

private:
    /// When `local_context` is set, fills S3 HTTP timeouts from ClickHouse settings.
    Lance::DatasetOptions getDatasetOptions(const ContextPtr & local_context = {}) const;

    StorageObjectStorageConfigurationWeakPtr configuration;
};

}

#endif
