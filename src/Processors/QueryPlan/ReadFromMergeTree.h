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
    ActionsDAGPtr filter_expression;
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

struct MergeTreeDataSelectAnalysisResult;
using MergeTreeDataSelectAnalysisResultPtr = std::shared_ptr<MergeTreeDataSelectAnalysisResult>;

/// This step is created to read from MergeTree* table.
/// For now, it takes a list of parts and creates source from it.
class ReadFromMergeTree final : public SourceStepWithFilter
{
public:

    enum class IndexType
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

    enum class ReadType
    {
        /// By default, read will use MergeTreeReadPool and return pipe with num_streams outputs.
        /// If num_streams == 1, will read without pool, in order specified in parts.
        Default,
        /// Read in sorting key order.
        /// Returned pipe will have the number of ports equals to parts.size().
        /// Parameter num_streams_ is ignored in this case.
        /// User should add MergingSorted itself if needed.
        InOrder,
        /// The same as InOrder, but in reverse order.
        /// For every part, read ranges and granules from end to begin. Also add ReverseTransform.
        InReverseOrder,
        /// A special type of reading where every replica
        /// talks to a remote coordinator (which is located on the initiator node)
        /// and who spreads marks and parts across them.
        ParallelReplicas,
    };

    struct AnalysisResult
    {
        RangesInDataParts parts_with_ranges;
        MergeTreeDataSelectSamplingData sampling;
        IndexStats index_stats;
        Names column_names_to_read;
        ReadFromMergeTree::ReadType read_type = ReadFromMergeTree::ReadType::Default;
        UInt64 total_parts = 0;
        UInt64 parts_before_pk = 0;
        UInt64 selected_parts = 0;
        UInt64 selected_ranges = 0;
        UInt64 selected_marks = 0;
        UInt64 selected_marks_pk = 0;
        UInt64 total_marks_pk = 0;
        UInt64 selected_rows = 0;

        void checkLimits(const Settings & settings, const SelectQueryInfo & query_info_) const;
    };

    ReadFromMergeTree(
        MergeTreeData::DataPartsVector parts_,
        std::vector<AlterConversionsPtr> alter_conversions_,
        Names real_column_names_,
        Names virt_column_names_,
        const MergeTreeData & data_,
        const SelectQueryInfo & query_info_,
        StorageSnapshotPtr storage_snapshot,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_,
        bool sample_factor_column_queried_,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
        Poco::Logger * log_,
        MergeTreeDataSelectAnalysisResultPtr analyzed_result_ptr_,
        bool enable_parallel_reading
    );

    static constexpr auto name = "ReadFromMergeTree";
    String getName() const override { return name; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(FormatSettings & format_settings) const override;
    void describeIndexes(FormatSettings & format_settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeIndexes(JSONBuilder::JSONMap & map) const override;

    const Names & getRealColumnNames() const { return real_column_names; }
    const Names & getVirtualColumnNames() const { return virt_column_names; }

    StorageID getStorageID() const { return data.getStorageID(); }
    const StorageSnapshotPtr & getStorageSnapshot() const { return storage_snapshot; }
    UInt64 getSelectedParts() const { return selected_parts; }
    UInt64 getSelectedRows() const { return selected_rows; }
    UInt64 getSelectedMarks() const { return selected_marks; }

    struct Indexes
    {
        KeyCondition key_condition;
        std::optional<PartitionPruner> partition_pruner;
        std::optional<KeyCondition> minmax_idx_condition;
        UsefulSkipIndexes skip_indexes;
        bool use_skip_indexes;
    };

    static MergeTreeDataSelectAnalysisResultPtr selectRangesToRead(
        MergeTreeData::DataPartsVector parts,
        std::vector<AlterConversionsPtr> alter_conversions,
        const PrewhereInfoPtr & prewhere_info,
        const ActionDAGNodes & added_filter_nodes,
        const StorageMetadataPtr & metadata_snapshot_base,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        size_t num_streams,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read,
        const MergeTreeData & data,
        const Names & real_column_names,
        bool sample_factor_column_queried,
        Poco::Logger * log,
        std::optional<Indexes> & indexes);

    MergeTreeDataSelectAnalysisResultPtr selectRangesToRead(
        MergeTreeData::DataPartsVector parts,
        std::vector<AlterConversionsPtr> alter_conversions) const;

    ContextPtr getContext() const { return context; }
    const SelectQueryInfo & getQueryInfo() const { return query_info; }
    StorageMetadataPtr getStorageMetadata() const { return metadata_for_reading; }
    const PrewhereInfoPtr & getPrewhereInfo() const { return prewhere_info; }

    /// Returns `false` if requested reading cannot be performed.
    bool requestReadingInOrder(size_t prefix_size, int direction, size_t limit);
    bool readsInOrder() const;

    void updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value);
    bool isQueryWithFinal() const;
    bool isQueryWithSampling() const;

    /// Returns true if the optimisation is applicable (and applies it then).
    bool requestOutputEachPartitionThroughSeparatePort();
    bool willOutputEachPartitionThroughSeparatePort() const { return output_each_partition_through_separate_port; }

    bool hasAnalyzedResult() const { return analyzed_result_ptr != nullptr; }
    void setAnalyzedResult(MergeTreeDataSelectAnalysisResultPtr analyzed_result_ptr_) { analyzed_result_ptr = std::move(analyzed_result_ptr_); }

    void resetParts(MergeTreeData::DataPartsVector parts)
    {
        prepared_parts = std::move(parts);
        alter_conversions_for_parts = {};
    }

    const MergeTreeData::DataPartsVector & getParts() const { return prepared_parts; }
    const MergeTreeData & getMergeTreeData() const { return data; }
    size_t getMaxBlockSize() const { return max_block_size; }
    size_t getNumStreams() const { return requested_num_streams; }
    bool isParallelReadingEnabled() const { return read_task_callback != std::nullopt; }

    void applyFilters() override;

private:
    static MergeTreeDataSelectAnalysisResultPtr selectRangesToReadImpl(
        MergeTreeData::DataPartsVector parts,
        std::vector<AlterConversionsPtr> alter_conversions,
        const StorageMetadataPtr & metadata_snapshot_base,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        size_t num_streams,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read,
        const MergeTreeData & data,
        const Names & real_column_names,
        bool sample_factor_column_queried,
        Poco::Logger * log,
        std::optional<Indexes> & indexes);

    int getSortDirection() const
    {
        const InputOrderInfoPtr & order_info = query_info.getInputOrderInfo();
        if (order_info)
            return order_info->direction;

        return 1;
    }

    MergeTreeReaderSettings reader_settings;

    MergeTreeData::DataPartsVector prepared_parts;
    std::vector<AlterConversionsPtr> alter_conversions_for_parts;

    Names real_column_names;
    Names virt_column_names;

    const MergeTreeData & data;
    SelectQueryInfo query_info;
    PrewhereInfoPtr prewhere_info;
    ExpressionActionsSettings actions_settings;

    StorageSnapshotPtr storage_snapshot;
    StorageMetadataPtr metadata_for_reading;

    ContextPtr context;

    const size_t max_block_size;
    size_t requested_num_streams;
    size_t output_streams_limit = 0;
    const size_t preferred_block_size_bytes;
    const size_t preferred_max_column_in_block_size_bytes;
    const bool sample_factor_column_queried;

    /// Used for aggregation optimisation (see DB::QueryPlanOptimizations::tryAggregateEachPartitionIndependently).
    bool output_each_partition_through_separate_port = false;

    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read;

    /// Pre-computed value, needed to trigger sets creating for PK
    mutable std::optional<Indexes> indexes;

    Poco::Logger * log;
    UInt64 selected_parts = 0;
    UInt64 selected_rows = 0;
    UInt64 selected_marks = 0;

    Pipe read(RangesInDataParts parts_with_range, Names required_columns, ReadType read_type, size_t max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache);
    Pipe readFromPool(RangesInDataParts parts_with_ranges, Names required_columns, size_t max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache);
    Pipe readFromPoolParallelReplicas(RangesInDataParts parts_with_ranges, Names required_columns, size_t max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache);
    Pipe readInOrder(RangesInDataParts parts_with_range, Names required_columns, ReadType read_type, bool use_uncompressed_cache, UInt64 limit, MergeTreeInOrderReadPoolParallelReplicasPtr pool);

    template<typename TSource>
    ProcessorPtr createSource(const RangesInDataPart & part, const Names & required_columns, bool use_uncompressed_cache, bool has_limit_below_one_block, MergeTreeInOrderReadPoolParallelReplicasPtr pool);

    Pipe spreadMarkRanges(
        RangesInDataParts && parts_with_ranges, size_t num_streams, AnalysisResult & result, ActionsDAGPtr & result_projection);

    Pipe groupStreamsByPartition(AnalysisResult & result, ActionsDAGPtr & result_projection);

    Pipe spreadMarkRangesAmongStreams(RangesInDataParts && parts_with_ranges, size_t num_streams, const Names & column_names);

    Pipe spreadMarkRangesAmongStreamsWithOrder(
        RangesInDataParts && parts_with_ranges,
        size_t num_streams,
        const Names & column_names,
        ActionsDAGPtr & out_projection,
        const InputOrderInfoPtr & input_order_info);

    Pipe spreadMarkRangesAmongStreamsFinal(
        RangesInDataParts && parts, size_t num_streams, const Names & column_names, ActionsDAGPtr & out_projection);

    ReadFromMergeTree::AnalysisResult getAnalysisResult() const;
    MergeTreeDataSelectAnalysisResultPtr analyzed_result_ptr;

    bool is_parallel_reading_from_replicas;
    std::optional<MergeTreeAllRangesCallback> all_ranges_callback;
    std::optional<MergeTreeReadTaskCallback> read_task_callback;
};

struct MergeTreeDataSelectAnalysisResult
{
    std::variant<std::exception_ptr, ReadFromMergeTree::AnalysisResult> result;

    bool error() const;
    size_t marks() const;
};

}
