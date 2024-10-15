#pragma once
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/PartitionPruner.h>

namespace DB
{

using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;

class Pipe;

using MergeTreeReadTaskCallback = std::function<std::optional<ParallelReadResponse>(ParallelReadRequest)>;

struct MergeTreeDataSelectSamplingData
{
    bool use_sampling = false;
    bool read_nothing = false;
    Float64 used_sample_factor = 1.0;
    std::shared_ptr<ASTFunction> filter_function;
    std::shared_ptr<const ActionsDAG> filter_expression;
};

struct UsefulSkipIndexes
{
    struct DataSkippingIndexAndCondition
    {
        MergeTreeIndexPtr index;
        MergeTreeIndexConditionPtr condition;

        DataSkippingIndexAndCondition(MergeTreeIndexPtr index_, MergeTreeIndexConditionPtr condition_)
            : index(index_), condition(condition_)
        {
        }
    };

    struct MergedDataSkippingIndexAndCondition
    {
        std::vector<MergeTreeIndexPtr> indices;
        MergeTreeIndexMergedConditionPtr condition;

        void addIndex(const MergeTreeIndexPtr & index)
        {
            indices.push_back(index);
            condition->addIndex(indices.back());
        }
    };

    std::vector<DataSkippingIndexAndCondition> useful_indices;
    std::vector<MergedDataSkippingIndexAndCondition> merged_indices;
};

/// This step is created to read from MergeTree* table.
/// For now, it takes a list of parts and creates source from it.
class ReadFromMergeTree final : public SourceStepWithFilter
{
public:
    enum class IndexType : uint8_t
    {
        None,
        MinMax,
        Partition,
        PrimaryKey,
        Skip,
    };

    /// This is a struct with information about applied indexes.
    /// Is used for introspection only, in EXPLAIN query.
    struct IndexStat
    {
        IndexType type;
        std::string name = {};
        std::string description = {};
        std::string condition = {};
        std::vector<std::string> used_keys = {};
        size_t num_parts_after;
        size_t num_granules_after;
    };

    using IndexStats = std::vector<IndexStat>;
    using ReadType = MergeTreeReadType;

    struct AnalysisResult
    {
        RangesInDataParts parts_with_ranges;
        MergeTreeDataSelectSamplingData sampling;
        IndexStats index_stats;
        Names column_names_to_read;
        ReadType read_type = ReadType::Default;
        UInt64 total_parts = 0;
        UInt64 parts_before_pk = 0;
        UInt64 selected_parts = 0;
        UInt64 selected_ranges = 0;
        UInt64 selected_marks = 0;
        UInt64 selected_marks_pk = 0;
        UInt64 total_marks_pk = 0;
        UInt64 selected_rows = 0;
        bool has_exact_ranges = false;

        bool readFromProjection() const { return !parts_with_ranges.empty() && parts_with_ranges.front().data_part->isProjectionPart(); }
        void checkLimits(const Settings & settings, const SelectQueryInfo & query_info_) const;
    };

    using AnalysisResultPtr = std::shared_ptr<AnalysisResult>;

    ReadFromMergeTree(
        MergeTreeData::DataPartsVector parts_,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
        Names all_column_names_,
        const MergeTreeData & data_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context_,
        size_t max_block_size_,
        size_t num_streams_,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
        LoggerPtr log_,
        AnalysisResultPtr analyzed_result_ptr_,
        bool enable_parallel_reading_,
        std::optional<MergeTreeAllRangesCallback> all_ranges_callback_ = std::nullopt,
        std::optional<MergeTreeReadTaskCallback> read_task_callback_ = std::nullopt,
        std::optional<size_t> number_of_current_replica_ = std::nullopt);

    std::unique_ptr<ReadFromMergeTree> createLocalParallelReplicasReadingStep(
        AnalysisResultPtr analyzed_result_ptr_,
        MergeTreeAllRangesCallback all_ranges_callback_,
        MergeTreeReadTaskCallback read_task_callback_,
        size_t replica_number);

    static constexpr auto name = "ReadFromMergeTree";
    String getName() const override { return name; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(FormatSettings & format_settings) const override;
    void describeIndexes(FormatSettings & format_settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeIndexes(JSONBuilder::JSONMap & map) const override;

    const Names & getAllColumnNames() const { return all_column_names; }

    StorageID getStorageID() const { return data.getStorageID(); }
    UInt64 getSelectedParts() const { return selected_parts; }
    UInt64 getSelectedRows() const { return selected_rows; }
    UInt64 getSelectedMarks() const { return selected_marks; }

    struct Indexes
    {
        explicit Indexes(KeyCondition key_condition_)
            : key_condition(std::move(key_condition_))
            , use_skip_indexes(false)
        {}

        KeyCondition key_condition;
        std::optional<PartitionPruner> partition_pruner;
        std::optional<KeyCondition> minmax_idx_condition;
        std::optional<KeyCondition> part_offset_condition;
        UsefulSkipIndexes skip_indexes;
        bool use_skip_indexes;
        std::optional<std::unordered_set<String>> part_values;
    };

    static AnalysisResultPtr selectRangesToRead(
        MergeTreeData::DataPartsVector parts,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        size_t num_streams,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read,
        const MergeTreeData & data,
        const Names & all_column_names,
        LoggerPtr log,
        std::optional<Indexes> & indexes,
        bool find_exact_ranges);

    AnalysisResultPtr selectRangesToRead(MergeTreeData::DataPartsVector parts, bool find_exact_ranges = false) const;

    AnalysisResultPtr selectRangesToRead(bool find_exact_ranges = false) const;

    StorageMetadataPtr getStorageMetadata() const { return storage_snapshot->metadata; }

    /// Returns `false` if requested reading cannot be performed.
    bool requestReadingInOrder(size_t prefix_size, int direction, size_t limit);
    bool readsInOrder() const;
    const InputOrderInfoPtr & getInputOrder() const { return query_info.input_order_info; }
    const SortDescription & getSortDescription() const override { return result_sort_description; }

    void updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value) override;
    bool isQueryWithSampling() const;

    /// Returns true if the optimization is applicable (and applies it then).
    bool requestOutputEachPartitionThroughSeparatePort();
    bool willOutputEachPartitionThroughSeparatePort() const { return output_each_partition_through_separate_port; }

    AnalysisResultPtr getAnalyzedResult() const { return analyzed_result_ptr; }
    void setAnalyzedResult(AnalysisResultPtr analyzed_result_ptr_) { analyzed_result_ptr = std::move(analyzed_result_ptr_); }

    const MergeTreeData::DataPartsVector & getParts() const { return prepared_parts; }
    MergeTreeData::MutationsSnapshotPtr getMutationsSnapshot() const { return mutations_snapshot; }

    const MergeTreeData & getMergeTreeData() const { return data; }
    size_t getMaxBlockSize() const { return block_size.max_block_size_rows; }
    size_t getNumStreams() const { return requested_num_streams; }
    bool isParallelReadingEnabled() const { return read_task_callback != std::nullopt; }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    MergeTreeReaderSettings reader_settings;

    MergeTreeData::DataPartsVector prepared_parts;
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot;

    Names all_column_names;

    const MergeTreeData & data;
    ExpressionActionsSettings actions_settings;

    const MergeTreeReadTask::BlockSizeParams block_size;

    SortDescription result_sort_description;

    size_t requested_num_streams;
    size_t output_streams_limit = 0;

    /// Used for aggregation optimization (see DB::QueryPlanOptimizations::tryAggregateEachPartitionIndependently).
    bool output_each_partition_through_separate_port = false;

    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read;

    /// Pre-computed value, needed to trigger sets creating for PK
    mutable std::optional<Indexes> indexes;

    LoggerPtr log;
    UInt64 selected_parts = 0;
    UInt64 selected_rows = 0;
    UInt64 selected_marks = 0;

    using PoolSettings = MergeTreeReadPoolBase::PoolSettings;

    Pipe read(RangesInDataParts parts_with_range, Names required_columns, ReadType read_type, size_t max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache);
    Pipe readFromPool(RangesInDataParts parts_with_range, Names required_columns, PoolSettings pool_settings);
    Pipe readFromPoolParallelReplicas(RangesInDataParts parts_with_range, Names required_columns, PoolSettings pool_settings);
    Pipe readInOrder(RangesInDataParts parts_with_ranges, Names required_columns, PoolSettings pool_settings, ReadType read_type, UInt64 limit);

    Pipe spreadMarkRanges(RangesInDataParts && parts_with_ranges, size_t num_streams, AnalysisResult & result, std::optional<ActionsDAG> & result_projection);

    Pipe groupStreamsByPartition(AnalysisResult & result, std::optional<ActionsDAG> & result_projection);

    Pipe spreadMarkRangesAmongStreams(RangesInDataParts && parts_with_ranges, size_t num_streams, const Names & column_names);

    Pipe spreadMarkRangesAmongStreamsWithOrder(
        RangesInDataParts && parts_with_ranges,
        size_t num_streams,
        const Names & column_names,
        std::optional<ActionsDAG> & out_projection,
        const InputOrderInfoPtr & input_order_info);

    bool doNotMergePartsAcrossPartitionsFinal() const;

    Pipe spreadMarkRangesAmongStreamsFinal(
        RangesInDataParts && parts, size_t num_streams, const Names & origin_column_names, const Names & column_names, std::optional<ActionsDAG> & out_projection);

    ReadFromMergeTree::AnalysisResult getAnalysisResult() const;

    int getSortDirection() const;
    void updateSortDescription();

    mutable AnalysisResultPtr analyzed_result_ptr;
    VirtualFields shared_virtual_fields;

    bool is_parallel_reading_from_replicas;
    std::optional<MergeTreeAllRangesCallback> all_ranges_callback;
    std::optional<MergeTreeReadTaskCallback> read_task_callback;
    bool enable_vertical_final = false;
    bool enable_remove_parts_from_snapshot_optimization = true;
    std::optional<size_t> number_of_current_replica;
};

}
