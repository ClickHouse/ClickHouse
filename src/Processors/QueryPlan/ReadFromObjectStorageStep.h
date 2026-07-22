#pragma once

#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class ReadFromObjectStorageStep : public SourceStepWithFilter
{
public:
    ReadFromObjectStorageStep(
        const StorageID & storage_id_,
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
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

    static constexpr auto STEP_NAME = "ReadFromObjectStorage";

    std::string getName() const override { return STEP_NAME; }

    StorageMetadataPtr getStorageMetadata() const { return storage_snapshot->metadata; }


    void applyFilters(ActionDAGNodes added_filter_nodes) override;
    void updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value) override;

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    QueryPlanStepPtr clone() const override;
#if CLICKHOUSE_CLOUD
    /// In distributed query plan, this step will be executed in a distributed manner - shards will be read in parallel.
    void setDistributedRead(size_t bucket_count);
    Strings getShardsForDistributedRead() const;

    void serialize(Serialization & ctx) const override;
    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);
#endif

    bool requestReadingInOrder() const;

    // The name of the returned type is misleading, this order has nothing in common with the corresponding SELECT query
    // and is taken from the storage metadata.
    InputOrderInfoPtr getDataOrder() const;

private:
    StorageID storage_id;
    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationPtr configuration;
    std::shared_ptr<IObjectIterator> iterator_wrapper;

    ReadFromFormatInfo info;
    const NamesAndTypesList virtual_columns;
    const std::optional<DB::FormatSettings> format_settings;
    const bool need_only_count;
    const size_t max_block_size;
    size_t num_streams;
    const size_t max_num_streams;
    const bool distributed_processing;
#if CLICKHOUSE_CLOUD
    /// This is set when this step is part of a distributed query plan and it will be executed in a distributed manner.
    /// "bucket_id" task parameter will be used to determine what part of the data to read.
    size_t distributed_read_bucket_count = 0;

    std::optional<size_t> total_buckets;
    std::optional<size_t> our_bucket;
#endif

    void createIterator();
};

}
