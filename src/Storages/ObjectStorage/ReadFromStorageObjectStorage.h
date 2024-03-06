#pragma once
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/StorageObjectStorageQuerySettings.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

namespace DB
{

class ReadFromStorageObejctStorage : public SourceStepWithFilter
{
public:
    using ConfigurationPtr = StorageObjectStorageConfigurationPtr;

    ReadFromStorageObejctStorage(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr configuration_,
        const String & name_,
        const NamesAndTypesList & virtual_columns_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const std::optional<DB::FormatSettings> & format_settings_,
        const StorageObjectStorageSettings & query_settings_,
        bool distributed_processing_,
        ReadFromFormatInfo info_,
        SchemaCache & schema_cache_,
        bool need_only_count_,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_,
        CurrentMetrics::Metric metric_threads_count_,
        CurrentMetrics::Metric metric_threads_active_,
        CurrentMetrics::Metric metric_threads_scheduled_);

    std::string getName() const override { return name; }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    ObjectStoragePtr object_storage;
    ConfigurationPtr configuration;
    std::shared_ptr<StorageObjectStorageSource::IIterator> iterator_wrapper;

    const ReadFromFormatInfo info;
    const NamesAndTypesList virtual_columns;
    const std::optional<DB::FormatSettings> format_settings;
    const StorageObjectStorageSettings query_settings;
    SchemaCache & schema_cache;
    const String name;
    const bool need_only_count;
    const size_t max_block_size;
    const size_t num_streams;
    const bool distributed_processing;
    const CurrentMetrics::Metric metric_threads_count;
    const CurrentMetrics::Metric metric_threads_active;
    const CurrentMetrics::Metric metric_threads_scheduled;

    void createIterator(const ActionsDAG::Node * predicate);
};

}
