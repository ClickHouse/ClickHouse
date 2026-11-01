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
struct ObjectStorageTopKQueryConditionCacheKey;
using ObjectStorageTopKQueryConditionCacheKeyPtr = std::shared_ptr<const ObjectStorageTopKQueryConditionCacheKey>;

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
    const StorageID & getStorageID() const { return storage_id; }

    /// Rows of this read estimated from the data lake metadata, pruned by the step's filter. std::nullopt when the
    /// storage gives no estimate and with distributed processing.
    std::optional<DataLakeReadEstimate> estimateReadFromDataLakeMetadata() const;

    void applyFilters(ActionDAGNodes added_filter_nodes) override;
    void updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value) override;
    bool canUpdatePrewhereInfoMultipleTimes() const override { return false; }

    /// TopN dynamic filtering: only the Parquet reader consumes `FormatFilterInfo::top_k_filter`,
    /// and only for a sort column it physically reads from the file. Whether a particular file may
    /// apply it is decided per file by `StorageObjectStorageSource` (data lake schema evolution and
    /// identity partitions rewrite the values the reader returns).
    bool supportsTopKDynamicFilter(const ColumnWithTypeAndName & sort_column) const override;
    void setTopKFilter(std::shared_ptr<const FormatTopKFilterInfo> info_) override { top_k_filter = std::move(info_); }
    bool isSelectedForTopKFilterOptimization() const { return top_k_filter != nullptr; }
    /// A filter above the read whose contents the key does not cover (a join runtime filter) changes the
    /// threshold of the TopN filter, so the read must neither consult nor write the TopK cache entries.
    void disableTopKQueryConditionCache() { allow_top_k_query_condition_cache = false; }

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

    bool requestReadingInOrder() const;

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
    /// The columns `keepOnlyRequiredColumnsAndCreateLazyReadStep` would move to the lazy read, without moving
    /// them.
    NameSet getLazilyReadColumns(const NameSet & required_names) const;

    LazyObjectStorageFileRegistryPtr getLazyRowIndexRegistry() const { return lazy_row_index_registry; }

private:
    /// What the main read keeps besides `required_names` when columns are split off.
    NameSet getNamesToKeepInMainRead(const NameSet & required_names) const;

    StorageID storage_id;
    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationPtr configuration;
    std::shared_ptr<IObjectIterator> iterator_wrapper;

    /// Memoized `estimateReadFromDataLakeMetadata`, which walks the manifests; reset when the filter changes.
    /// A storage without an estimate returns nullopt without any work, so nullopt is not memoized.
    mutable std::optional<DataLakeReadEstimate> data_lake_read_estimate;

    /// Lazy materialization: set iff keepOnlyRequiredColumnsAndCreateLazyReadStep was called.
    LazyObjectStorageFileRegistryPtr lazy_row_index_registry;

    ReadFromFormatInfo info;
    const NamesAndTypesList virtual_columns;
    const std::optional<DB::FormatSettings> format_settings;
    const bool need_only_count;
    const size_t max_block_size;
    size_t num_streams;
    const size_t max_num_streams;
    const bool distributed_processing;
    std::shared_ptr<const FormatTopKFilterInfo> top_k_filter;
    bool allow_top_k_query_condition_cache = true;
#if CLICKHOUSE_CLOUD
    /// This is set when this step is part of a distributed query plan and it will be executed in a distributed manner.
    /// "bucket_id" task parameter will be used to determine what part of the data to read.
    size_t distributed_read_bucket_count = 0;

    std::optional<size_t> total_buckets;
    std::optional<size_t> our_bucket;
#endif

    void createIterator();

    ObjectStorageTopKQueryConditionCacheKeyPtr makeTopKQueryConditionCacheKey() const;
};

}
