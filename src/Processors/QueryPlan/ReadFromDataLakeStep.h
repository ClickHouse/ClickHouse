#pragma once

#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageTableOptions.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class IDataLakeMetadata;

class ReadFromDataLakeStep : public SourceStepWithFilter
{
public:
    ReadFromDataLakeStep(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        const StorageObjectStorageTableOptions & table_options_,
        const Names & columns_to_read,
        const NamesAndTypesList & virtual_columns_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const std::optional<DB::FormatSettings> & format_settings_,
        ReadFromFormatInfo info_,
        bool need_only_count_,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_,
        IDataLakeMetadata * metadata_,
        bool distributed_processing_);

    static constexpr auto STEP_NAME = "ReadFromDataLake";

    std::string getName() const override { return STEP_NAME; }

    StorageMetadataPtr getStorageMetadata() const { return storage_snapshot->metadata; }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;
    void updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value) override;

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    QueryPlanStepPtr clone() const override;

    bool requestReadingInOrder() const;

    // The name of the returned type is misleading, this order has nothing in common with the corresponding SELECT query
    // and is taken from the storage metadata.
    InputOrderInfoPtr getDataOrder() const;

private:
    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationPtr configuration;
    const StorageObjectStorageTableOptions table_options;
    std::shared_ptr<IObjectIterator> iterator_wrapper;

    ReadFromFormatInfo info;
    const NamesAndTypesList virtual_columns;
    const std::optional<DB::FormatSettings> format_settings;
    const bool need_only_count;
    const size_t max_block_size;
    size_t num_streams;
    const size_t max_num_streams;

    /// Non-owning pointer to datalake metadata, owned by StorageDataLake.
    IDataLakeMetadata * metadata;

    const bool distributed_processing;

    void createIterator();
};

}
