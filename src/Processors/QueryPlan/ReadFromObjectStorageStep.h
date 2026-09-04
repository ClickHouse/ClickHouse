#pragma once

#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class LazilyReadFromObjectStorage;
struct LazyObjectStorageFileRegistry;
using LazyObjectStorageFileRegistryPtr = std::shared_ptr<LazyObjectStorageFileRegistry>;

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
    bool canUpdatePrewhereInfoMultipleTimes() const override { return false; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    QueryPlanStepPtr clone() const override;
#if CLICKHOUSE_CLOUD
    /// In distributed query plan, this step will be executed in a distributed manner - shards will be read in parallel.
    void setDistributedRead(size_t bucket_count);
    Strings getShardsForDistributedRead() const;

    bool isSerializable() const override { return true; }
    void serialize(Serialization & ctx) const override;
    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);
#endif

    /// Returns true only if the pipeline will deliver one sorted run per output port. Decides
    /// everything here: it runs during optimization, and the caller drops the sorting step on true.
    bool requestReadingInOrder(int direction, const QueryPlanOptimizationSettings & optimization_settings);

    // The name of the returned type is misleading, this order has nothing in common with the corresponding SELECT query
    // and is taken from the storage metadata.
    InputOrderInfoPtr getDataOrder() const;

    /// Lazy materialization support (see optimizeLazyMaterialization2).
    bool canUseLazyMaterialization() const;

    /// Reduces the set of columns this step reads to `required_names` (plus the columns the
    /// PREWHERE / row-level filter needs, virtual columns and hive partition columns), makes the
    /// step append a `__global_row_index` column to the output, and returns a step that lazily
    /// reads the removed columns. Returns nullptr if there is nothing to defer.
    std::unique_ptr<LazilyReadFromObjectStorage> keepOnlyRequiredColumnsAndCreateLazyReadStep(const NameSet & required_names);

    LazyObjectStorageFileRegistryPtr getLazyRowIndexRegistry() const { return lazy_row_index_registry; }

private:
    StorageID storage_id;
    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationPtr configuration;
    std::shared_ptr<IObjectIterator> iterator_wrapper;

    /// Lazy materialization: set iff keepOnlyRequiredColumnsAndCreateLazyReadStep was called.
    LazyObjectStorageFileRegistryPtr lazy_row_index_registry;

    ReadFromFormatInfo info;
    const NamesAndTypesList virtual_columns;
    /// Not const: the in-order path pins Parquet's preserve_order, which is what makes a single
    /// file a sorted run at all.
    std::optional<DB::FormatSettings> format_settings;
    const bool need_only_count;
    const size_t max_block_size;
    size_t num_streams;
    const size_t max_num_streams;
    const bool distributed_processing;

    /// Set by a successful requestReadingInOrder. `read_in_order_files` is the file list
    /// enumerated there, so initializePipeline consumes it instead of re-deciding.
    /// `read_in_order_attempted` keeps the enumeration, which consumes the iterator, single-shot.
    bool read_in_order_attempted = false;
    bool read_in_order = false;
    ObjectInfos read_in_order_files;
#if CLICKHOUSE_CLOUD
    /// This is set when this step is part of a distributed query plan and it will be executed in a distributed manner.
    /// "bucket_id" task parameter will be used to determine what part of the data to read.
    size_t distributed_read_bucket_count = 0;

    std::optional<size_t> total_buckets;
    std::optional<size_t> our_bucket;
#endif

    void createIterator();
    bool sortingKeyIsComputableFromSourceHeader() const;
    Pipe buildInOrderPipe(const ContextPtr & local_context, const FormatFilterInfoPtr & format_filter_info);
};

}
