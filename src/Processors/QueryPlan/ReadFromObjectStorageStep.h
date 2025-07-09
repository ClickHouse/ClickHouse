#pragma once

#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>

namespace DB
{

class ReadFromObjectStorageStep : public SourceStepWithFilter
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    ReadFromObjectStorageStep(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr configuration_,
        const String & name_,
        const Names & columns_to_read,
        const NamesAndTypesList & virtual_columns_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const std::optional<DB::FormatSettings> & format_settings_,
        bool distributed_processing_,
        ReadFromFormatInfo info_,
        bool need_only_count_,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_);

    std::string getName() const override { return name; }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    ObjectStoragePtr object_storage;
    ConfigurationPtr configuration;
    std::shared_ptr<IObjectIterator> iterator_wrapper;

    const ReadFromFormatInfo info;
    const NamesAndTypesList virtual_columns;
    const std::optional<DB::FormatSettings> format_settings;
    const std::string name;
    const bool need_only_count;
    const size_t max_block_size;
    size_t num_streams;
    const bool distributed_processing;

    void createIterator();
};

}
