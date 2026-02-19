#pragma once
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <Processors/TopKThresholdTracker.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

class Pipe;
class ParallelReadingExtension;

using MergeTreeReadTaskCallback = std::function<std::optional<ParallelReadResponse>(ParallelReadRequest)>;

using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;
using PartitionIdToMaxBlockPtr = std::shared_ptr<const PartitionIdToMaxBlock>;

class LazilyReadFromMergeTree;
struct QueryIdHolder;

struct MergeTreeDataSelectSamplingData
{
    bool use_sampling = false;
    bool read_nothing = false;
    Float64 used_sample_factor = 1.0;
    boost::intrusive_ptr<ASTFunction> filter_function;
    std::shared_ptr<const ActionsDAG> filter_expression;
};

struct UsefulSkipIndexes
{
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

    bool empty() const { return useful_indices.empty() && merged_indices.empty() && !skip_index_for_top_k_filtering; }

    std::vector<MergeTreeIndexWithCondition> useful_indices;
    std::vector<MergedDataSkippingIndexAndCondition> merged_indices;
    std::vector<std::vector<size_t>> per_part_index_orders;
    MergeTreeIndexPtr skip_index_for_top_k_filtering{nullptr};
    TopKThresholdTrackerPtr threshold_tracker{nullptr};
};

/// Contains parts each from different projection index
using ProjectionIndexReadRangesByIndex = std::unordered_map<size_t, RangesInDataParts>;

struct ProjectionIndexReadInfo
{
    ProjectionDescriptionRawPtr projection;
    PrewhereInfoPtr prewhere_info;
};
using ProjectionIndexReadInfos = std::vector<ProjectionIndexReadInfo>;

struct ProjectionIndexReadDescription
{
    ProjectionIndexReadRangesByIndex read_ranges;
    ProjectionIndexReadInfos read_infos;
};

struct MergeTreeIndexBuildContext;
using MergeTreeIndexBuildContextPtr = std::shared_ptr<MergeTreeIndexBuildContext>;

struct TopKFilterInfo
{
    String column_name;
    DataTypePtr data_type;
    size_t limit_n;
    int direction; /// 1 = ASC, -1 = DESC
    bool where_clause;
    TopKThresholdTrackerPtr threshold_tracker;
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
        PrimaryKeyExpand,
    };

    struct DistributedIndexStat
    {
        std::string address;
        size_t num_parts_send;
        size_t num_parts_received;
        size_t num_granules_send;
        size_t num_granules_received;
        /// Note, probably need to include the following as well:
        /// - search_algorithm
    };

    /// This is a struct with information about applied indexes.
    /// Is used for introspection only, in EXPLAIN query.
    struct IndexStat
    {
        IndexType type;
        std::string name = {};
        std::string part_name = {};
        std::string description = {};
        std::string condition = {};
        std::vector<std::string> used_keys = {};
        size_t num_parts_after;
        size_t num_granules_after;
        MarkRanges::SearchAlgorithm search_algorithm = {MarkRanges::SearchAlgorithm::Unknown};

        std::vector<DistributedIndexStat> distributed = {};
    };

    using IndexStats = std::vector<IndexStat>;

    /// Information about used projections.
    struct ProjectionStat
    {
        std::string name = {};
        std::string description = {};
        std::string condition = {};
        MarkRanges::SearchAlgorithm search_algorithm = {MarkRanges::SearchAlgorithm::Unknown};
        UInt64 selected_parts = 0;
        UInt64 selected_ranges = 0;
        UInt64 selected_marks = 0;
        UInt64 selected_rows = 0;
        UInt64 filtered_parts = 0;
    };

    /// `deque` is used to ensure stable addresses during projection analysis stats building.
    using ProjectionStats = std::deque<ProjectionStat>;

    using ReadType = MergeTreeReadType;

    struct AnalysisResult
    {
        RangesInDataParts parts_with_ranges;
        SplitPartsByRanges split_parts;
        MergeTreeDataSelectSamplingData sampling;
        IndexStats index_stats;
        ProjectionStats projection_stats;
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
        std::atomic<bool> exceeded_row_limits = false;

        AnalysisResult() = default;

        AnalysisResult(const AnalysisResult & other)
            : parts_with_ranges(other.parts_with_ranges)
            , split_parts(other.split_parts)
            , sampling(other.sampling)
            , index_stats(other.index_stats)
            , projection_stats(other.projection_stats)
            , column_names_to_read(other.column_names_to_read)
            , read_type(other.read_type)
            , total_parts(other.total_parts)
            , parts_before_pk(other.parts_before_pk)
            , selected_parts(other.selected_parts)
            , selected_ranges(other.selected_ranges)
            , selected_marks(other.selected_marks)
            , selected_marks_pk(other.selected_marks_pk)
            , total_marks_pk(other.total_marks_pk)
            , selected_rows(other.selected_rows)
            , has_exact_ranges(other.has_exact_ranges)
            , exceeded_row_limits(other.exceeded_row_limits.load())
        {}

        AnalysisResult(AnalysisResult && other) noexcept
            : parts_with_ranges(std::move(other.parts_with_ranges))
            , split_parts(std::move(other.split_parts))
            , sampling(std::move(other.sampling))
            , index_stats(std::move(other.index_stats))
            , projection_stats(std::move(other.projection_stats))
            , column_names_to_read(std::move(other.column_names_to_read))
            , read_type(other.read_type)
            , total_parts(other.total_parts)
            , parts_before_pk(other.parts_before_pk)
            , selected_parts(other.selected_parts)
            , selected_ranges(other.selected_ranges)
            , selected_marks(other.selected_marks)
            , selected_marks_pk(other.selected_marks_pk)
            , total_marks_pk(other.total_marks_pk)
            , selected_rows(other.selected_rows)
            , has_exact_ranges(other.has_exact_ranges)
            , exceeded_row_limits(other.exceeded_row_limits.load())
        {}

        bool readFromProjection() const { return !parts_with_ranges.empty() && parts_with_ranges.front().data_part->isProjectionPart(); }

        /// Check query limits: max_partitions_to_read, max_concurrent_queries.
        /// Also, return QueryIdHolder. If not null, we should keep it until query finishes.
        std::shared_ptr<QueryIdHolder>
        checkLimits(const Context & context_, const MergeTreeData & data_, const MergeTreeSettings & data_settings_) const;

        bool isUsable() const { return !exceeded_row_limits; }
    };

    using AnalysisResultPtr = std::shared_ptr<AnalysisResult>;

    ReadFromMergeTree(
        RangesInDataPartsPtr parts_,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
        Names all_column_names_,
        const MergeTreeData & data_,
        MergeTreeSettingsPtr data_settings_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context_,
        size_t max_block_size_,
        size_t num_streams_,
        PartitionIdToMaxBlockPtr max_block_numbers_to_read_,
        LoggerPtr log_,
        AnalysisResultPtr analyzed_result_ptr_,
        bool enable_parallel_reading_,
        std::optional<MergeTreeAllRangesCallback> all_ranges_callback_ = std::nullopt,
        std::optional<MergeTreeReadTaskCallback> read_task_callback_ = std::nullopt,
        std::optional<size_t> number_of_current_replica_ = std::nullopt);

    ReadFromMergeTree(const ReadFromMergeTree &) = default;
    ReadFromMergeTree(ReadFromMergeTree &&) noexcept = default;

    std::unique_ptr<ReadFromMergeTree> createLocalParallelReplicasReadingStep(
        ContextPtr & context_,
        AnalysisResultPtr analyzed_result_ptr_,
        MergeTreeAllRangesCallback all_ranges_callback_,
        MergeTreeReadTaskCallback read_task_callback_,
        size_t replica_number);

    static constexpr auto name = "ReadFromMergeTree";
    String getName() const override { return name; }

    QueryPlanStepPtr clone() const override;

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(FormatSettings & format_settings) const override;
    void describeIndexes(FormatSettings & format_settings) const override;
    void describeProjections(FormatSettings & format_settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeIndexes(JSONBuilder::JSONMap & map) const override;
    void describeProjections(JSONBuilder::JSONMap & map) const override;

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
            , use_skip_indexes_for_disjunctions(false)
            , use_skip_indexes_if_final_exact_mode(false)
            , use_skip_indexes_on_data_read(false)
        {}

        KeyCondition key_condition;
        std::optional<KeyCondition> key_condition_rpn_template; /// skeleton of the key condition without resolved columns
        std::optional<PartitionPruner> partition_pruner;
        std::optional<KeyCondition> minmax_idx_condition;
        std::optional<KeyCondition> part_offset_condition;
        std::optional<KeyCondition> total_offset_condition;
        UsefulSkipIndexes skip_indexes;
        bool use_skip_indexes;
        bool use_skip_indexes_for_disjunctions;
        bool use_skip_indexes_if_final_exact_mode;
        bool use_skip_indexes_on_data_read;
        std::optional<std::unordered_set<String>> part_values;
    };

    static AnalysisResultPtr selectRangesToRead(
        const RangesInDataParts & parts,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot,
        const std::optional<VectorSearchParameters> & vector_search_parameters,
        const std::optional<TopKFilterInfo> & top_k_filter_info,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        size_t num_streams,
        PartitionIdToMaxBlockPtr max_block_numbers_to_read,
        const MergeTreeData & data,
        const MergeTreeSettingsPtr & data_settings_,
        const Names & all_column_names,
        LoggerPtr log,
        std::optional<Indexes> & indexes,
        bool find_exact_ranges,
        bool is_parallel_reading_from_replicas_,
        bool allow_query_condition_cache_,
        bool supports_skip_indexes_on_data_read);

    static bool areSkipIndexColumnsInPrimaryKey(const Names & primary_key_columns, const UsefulSkipIndexes & skip_indexes, bool any_one);

    AnalysisResultPtr selectRangesToRead(bool find_exact_ranges = false) const;

    StorageMetadataPtr getStorageMetadata() const { return storage_snapshot->metadata; }

    /// Returns `false` if requested reading cannot be performed.
    bool requestReadingInOrder(size_t prefix_size, int direction, size_t limit);
    bool setVirtualRowConversions(ActionsDAG virtual_row_conversion_);
    bool readsInOrder() const;
    const InputOrderInfoPtr & getInputOrder() const { return query_info.input_order_info; }
    const SortDescription & getSortDescription() const override { return result_sort_description; }

    void updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value) override;
    bool isQueryWithSampling() const;

    /// Special stuff for vector search - replace vector column in read list with virtual "_distance" column
    void replaceVectorColumnWithDistanceColumn(const String & vector_column);
    bool isVectorColumnReplaced() const;

    /// Returns true if the optimization is applicable (and applies it then).
    bool requestOutputEachPartitionThroughSeparatePort();
    bool willOutputEachPartitionThroughSeparatePort() const { return output_each_partition_through_separate_port; }

    AnalysisResultPtr getAnalyzedResult() const { return analyzed_result_ptr; }
    void setAnalyzedResult(AnalysisResultPtr analyzed_result_ptr_) { analyzed_result_ptr = std::move(analyzed_result_ptr_); }

    const RangesInDataParts & getParts() const { return analyzed_result_ptr ? analyzed_result_ptr->parts_with_ranges : *prepared_parts; }
    MergeTreeData::MutationsSnapshotPtr getMutationsSnapshot() const { return mutations_snapshot; }

    const MergeTreeData & getMergeTreeData() const { return data; }
    size_t getMaxBlockSize() const { return block_size.max_block_size_rows; }
    size_t getNumStreams() const { return requested_num_streams; }
    bool isParallelReadingEnabled() const { return read_task_callback != std::nullopt; }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

    void setVectorSearchParameters(std::optional<VectorSearchParameters> && vector_search_parameters_) { vector_search_parameters = vector_search_parameters_; }
    std::optional<VectorSearchParameters> getVectorSearchParameters() const { return vector_search_parameters; }

    bool isParallelReadingFromReplicas() const { return is_parallel_reading_from_replicas; }
    void disableQueryConditionCache() { allow_query_condition_cache = false; }
    void disableMergeTreePartsSnapshotRemoval() { enable_remove_parts_from_snapshot_optimization = false; }

    /// After projection optimization, ReadFromMergeTree may be replaced with a new reading step, and the ParallelReadingExtension must be forwarded to the new step.
    /// Meanwhile, the ParallelReadingExtension originally in ReadFromMergeTree might be clear.
    void clearParallelReadingExtension();
    std::shared_ptr<ParallelReadingExtension> getParallelReadingExtension();

    bool supportsDataflowStatisticsCollection() const override { return !isQueryWithFinal(); }

    /// Adds virtual columns for reading from text index.
    /// Removes physical text columns that were eliminated by direct read from text index.
    void createReadTasksForTextIndex(const UsefulSkipIndexes & skip_indexes, const IndexReadColumns & added_columns, const Names & removed_columns, bool is_final);

    const std::optional<Indexes> & getIndexes() const { return indexes; }
    ConditionSelectivityEstimatorPtr getConditionSelectivityEstimator(const Names & required_columns) const;

    static void buildIndexes(
        std::optional<ReadFromMergeTree::Indexes> & indexes,
        const ActionsDAG * filter_actions_dag_,
        const MergeTreeData & data,
        const RangesInDataParts & parts,
        [[maybe_unused]] const std::optional<VectorSearchParameters> & vector_search_parameters,
        [[maybe_unused]] std::optional<TopKFilterInfo> top_k_filter_info,
        const ContextPtr & query_context,
        const SelectQueryInfo & query_info_,
        const StorageMetadataPtr & metadata_snapshot);

    void setTopKColumn(const TopKFilterInfo & top_k_filter_info_);
    bool isSkipIndexAvailableForTopK(const String & sort_column) const;
    const ProjectionIndexReadDescription & getProjectionIndexReadDescription() const { return projection_index_read_desc; }
    ProjectionIndexReadDescription & getProjectionIndexReadDescription() { return projection_index_read_desc; }

    bool canRemoveUnusedColumns() const override;
    RemovedUnusedColumns removeUnusedColumns(NameMultiSet required_outputs, bool remove_inputs) override;
    bool canRemoveColumnsFromOutput() const override;

    bool isSelectedForTopKFilterOptimization() const { return top_k_filter_info.has_value(); }

    std::unique_ptr<LazilyReadFromMergeTree> keepOnlyRequiredColumnsAndCreateLazyReadStep(const NameSet & required_outputs);
    void addStartingPartOffsetAndPartOffset(bool & added_part_starting_offset, bool & added_part_offset);

    void deferFiltersAfterFinalIfNeeded();

private:
    MergeTreeSettingsPtr data_settings;
    MergeTreeReaderSettings reader_settings;

    RangesInDataPartsPtr prepared_parts;
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

    PartitionIdToMaxBlockPtr max_block_numbers_to_read;

    /// Pre-computed value, needed to trigger sets creating for PK
    mutable std::optional<Indexes> indexes;

    /// Row policy / prewhere deferred to after FINAL, if needed
    FilterDAGInfoPtr deferred_row_level_filter;
    PrewhereInfoPtr deferred_prewhere_info;

    LoggerPtr log;
    UInt64 selected_parts = 0;
    UInt64 selected_rows = 0;
    UInt64 selected_marks = 0;

    std::optional<VectorSearchParameters> vector_search_parameters;

    using PoolSettings = MergeTreeReadPoolBase::PoolSettings;

    Pipe read(
        RangesInDataParts parts_with_range,
        const MergeTreeIndexBuildContextPtr & index_build_context,
        Names required_columns,
        ReadType read_type,
        size_t max_streams,
        size_t min_marks_for_concurrent_read,
        bool use_uncompressed_cache);

    Pipe readFromPool(
        RangesInDataParts parts_with_range,
        const MergeTreeIndexBuildContextPtr & index_build_context,
        Names required_columns,
        PoolSettings pool_settings);

    Pipe readFromPoolParallelReplicas(
        RangesInDataParts parts_with_range,
        const MergeTreeIndexBuildContextPtr & index_build_context,
        Names required_columns,
        PoolSettings pool_settings);

    Pipe readInOrder(
        RangesInDataParts parts_with_ranges,
        const MergeTreeIndexBuildContextPtr & index_build_context,
        Names required_columns,
        PoolSettings pool_settings,
        ReadType read_type,
        UInt64 limit);

    Pipe spreadMarkRanges(
        RangesInDataParts && parts_with_ranges,
        const MergeTreeIndexBuildContextPtr & index_build_context,
        size_t num_streams,
        AnalysisResult & result,
        std::optional<ActionsDAG> & result_projection);

    Pipe groupStreamsByPartition(
        AnalysisResult & result,
        const MergeTreeIndexBuildContextPtr & index_build_context,
        std::optional<ActionsDAG> & result_projection);

    Pipe readByLayers(
        const RangesInDataParts & parts_with_ranges,
        SplitPartsByRanges split_parts,
        const MergeTreeIndexBuildContextPtr & index_build_context,
        const Names & column_names,
        const InputOrderInfoPtr & input_order_info);

    Pipe spreadMarkRangesAmongStreams(
        RangesInDataParts && parts_with_ranges,
        const MergeTreeIndexBuildContextPtr & index_build_context,
        size_t num_streams,
        const Names & column_names);

    Pipe spreadMarkRangesAmongStreamsWithOrder(
        RangesInDataParts && parts_with_ranges,
        const MergeTreeIndexBuildContextPtr & index_build_context,
        size_t num_streams,
        const Names & column_names,
        std::optional<ActionsDAG> & out_projection,
        const InputOrderInfoPtr & input_order_info);

    bool doNotMergePartsAcrossPartitionsFinal() const;

    Pipe spreadMarkRangesAmongStreamsFinal(
        RangesInDataParts && parts,
        const MergeTreeIndexBuildContextPtr & index_build_context,
        size_t num_streams,
        const Names & origin_column_names,
        const Names & column_names,
        std::optional<ActionsDAG> & out_projection);

    ReadFromMergeTree::AnalysisResult & getAnalysisResultImpl() const;
    const ReadFromMergeTree::AnalysisResult & getAnalysisResult() const { return getAnalysisResultImpl(); }
    ReadFromMergeTree::AnalysisResult & getAnalysisResult() { return getAnalysisResultImpl(); }

    int getSortDirection() const;
    void updateSortDescription();

    bool isParallelReplicasLocalPlanForInitiator() const;
    bool supportsSkipIndexesOnDataRead() const;

    mutable AnalysisResultPtr analyzed_result_ptr;
    VirtualFields shared_virtual_fields;
    IndexReadTasks index_read_tasks;

    bool is_parallel_reading_from_replicas;
    std::optional<MergeTreeAllRangesCallback> all_ranges_callback;
    std::optional<MergeTreeReadTaskCallback> read_task_callback;
    bool enable_vertical_final = false;
    bool enable_remove_parts_from_snapshot_optimization = true;
    bool allow_query_condition_cache = true;

    ExpressionActionsPtr virtual_row_conversion;

    std::optional<size_t> number_of_current_replica;

    std::optional<TopKFilterInfo> top_k_filter_info;
    ProjectionIndexReadDescription projection_index_read_desc;
};

}
