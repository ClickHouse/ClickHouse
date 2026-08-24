#pragma once
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/MergeTreeFinalMerge.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
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
    bool empty() const { return useful_indices.empty() && !skip_index_for_top_k_filtering; }

    std::vector<MergeTreeIndexWithCondition> useful_indices;
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
    size_t num_sort_columns;
    size_t limit_n;
    int direction; /// 1 = ASC, -1 = DESC
    bool where_clause;
    TopKThresholdTrackerPtr threshold_tracker;

    /// Deterministic hash over the parameters that describe the TopK filter at planning time:
    /// `(column_name, type_name, limit_n, direction, num_sort_columns)`. Used as part of the
    /// query condition cache key so that QCC entries written under a TopK plan are partitioned
    /// by the TopK parameters and don't bleed across plans with different LIMIT, sort key, etc.
    UInt64 condition_hash = 0;
};

struct LazyMaterializingRows;
using LazyMaterializingRowsPtr = std::shared_ptr<LazyMaterializingRows>;

/// `DistributedReadBucket` and `buildDistributedFinalPipe` live in `MergeTreeFinalMerge.h`.

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
        Statistics,
        NonIntersectingSplit,
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

    /// Direct reads from a text index (see `createReadTasksForTextIndex`). The tasks are self-contained,
    /// so the get/set pair lets another step reading the same table (e.g. one built by lazy FINAL) reproduce them.
    const IndexReadTasks & getIndexReadTasks() const { return index_read_tasks; }
    void setIndexReadTasks(IndexReadTasks index_read_tasks_) { index_read_tasks = std::move(index_read_tasks_); }

    /// True if a coordinator-side snapshot boundary is pinned (e.g. select_sequential_consistency).
    /// Such a read cannot be distributed: a worker reads from its own snapshot and cannot reproduce it.
    bool hasPinnedBlockNumbers() const { return max_block_numbers_to_read != nullptr; }

    StorageID getStorageID() const { return data.getStorageID(); }
    UInt64 getSelectedParts() const { return selected_parts; }
    UInt64 getSelectedRows() const { return selected_rows; }
    UInt64 getSelectedMarks() const { return selected_marks; }

    struct Indexes
    {
        explicit Indexes(ConditionTemplate<KeyCondition>::Ptr key_condition_)
            : key_condition(std::move(key_condition_))
            , use_skip_indexes(false)
            , use_skip_indexes_for_disjunctions(false)
            , use_skip_indexes_if_final_exact_mode(false)
            , use_skip_indexes_on_data_read(false)
        {}

        ConditionTemplate<KeyCondition>::Ptr key_condition;
        ConditionTemplate<KeyCondition>::Ptr key_condition_rpn_template; /// skeleton of the key condition without resolved columns
        ConditionTemplate<KeyCondition>::Ptr minmax_idx_condition;
        ConditionTemplate<KeyCondition>::Ptr part_offset_condition;
        ConditionTemplate<KeyCondition>::Ptr total_offset_condition;
        std::optional<PartitionPruner> partition_pruner;
        UsefulSkipIndexes skip_indexes;
        bool use_skip_indexes;
        bool use_skip_indexes_for_disjunctions;
        bool use_skip_indexes_if_final_exact_mode;
        bool use_skip_indexes_on_data_read;
        std::optional<std::unordered_set<String>> part_values;
    };

    void addJoinRuntimeFilterIndexAnalysisOnDataRead(const String & filter_id, const String & column_name, const DataTypePtr & column_type);

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
        bool supports_skip_indexes_on_data_read,
        bool check_row_limits);


    AnalysisResultPtr selectRangesToRead(bool find_exact_ranges = false) const;
    /// Analyze ranges only for an intermediate cardinality estimate, without enforcing row limits
    /// or memoizing the result. The executed read analyzes again after its final mode is known.
    AnalysisResultPtr selectRangesToReadForEstimation() const;

    /// Analyze the ranges to read for a throwaway pre-plan estimate, without consulting or populating
    /// the query condition cache and without caching the analysis on the step. Used for the automatic
    /// parallel-replicas sizing of a query which may still become a TopK read: that estimate runs before
    /// `tryOptimizeTopK`, so it cannot know whether the `use_query_condition_cache_for_top_k` gate
    /// applies, and the read that actually executes analyzes again with the gate that matches its final
    /// shape.
    AnalysisResultPtr estimateRangesToReadWithoutQueryConditionCache() const;

    StorageMetadataPtr getStorageMetadata() const { return storage_snapshot->metadata; }

    /// The query condition cache is keyed by (table UUID, part name, condition hash), so it must not
    /// see filters whose value can change while that key stays the same: non-deterministic virtual
    /// columns (query-wide part numbering, catalog names, disk placement).
    static bool filterDependsOnNonDeterministicVirtuals(const VirtualColumnsDescription & virtuals, const SelectQueryInfo & query_info_);

    /// Returns `false` if requested reading cannot be performed.
    bool requestReadingInOrder(size_t prefix_size, int direction, size_t read_limit, size_t query_limit = 0);
    bool setVirtualRowConversions(ActionsDAG virtual_row_conversion_);
    void resetVirtualRowConversions() { virtual_row_conversion = nullptr; }
    bool readsInOrder() const;
    const InputOrderInfoPtr & getInputOrder() const { return query_info.input_order_info; }
    const SortDescription & getSortDescription() const override { return result_sort_description; }

    void updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value) override;
    bool isQueryWithSampling() const;

    /// Special stuff for vector search - replace vector column in read list with virtual "_distance" column
    void replaceVectorColumnWithDistanceColumn(const String & vector_column);
    bool isVectorColumnReplaced() const;

    /// Add one more column (or subcolumn) to the read list, recomputing the output header. Used by the brute-force
    /// vector-search rewrite to additionally read the quantized-codes subcolumn of a vector column.
    void addReadColumn(const String & column);

    /// Returns true if the optimization is applicable (and applies it then).
    bool requestOutputEachPartitionThroughSeparatePortForAggregation();
    bool requestOutputEachPartitionThroughSeparatePortForLimitBy();
    void requestOutputEachPartitionThroughSeparatePortForDistinct();
    void requestOutputEachPartitionThroughSeparatePortForWindow();
    bool requestOutputEachPartitionThroughSeparatePortForCreatingSet();

    bool willOutputEachPartitionThroughSeparatePort() const { return output_each_partition_through_separate_port; }

    /// Cost heuristic for per-partition (independent) processing, shared by GROUP BY, DISTINCT and
    /// window functions.
    enum class ProcessorKind : uint8_t { Aggregation, Distinct, Window };
    bool isPartitionIndependentProcessingProfitable(ProcessorKind kind) const;

    AnalysisResultPtr getAnalyzedResult() const { return analyzed_result_ptr; }
    void setAnalyzedResult(AnalysisResultPtr analyzed_result_ptr_) { analyzed_result_ptr = std::move(analyzed_result_ptr_); }

    const RangesInDataParts & getParts() const { return analyzed_result_ptr ? analyzed_result_ptr->parts_with_ranges : *prepared_parts; }
    MergeTreeData::MutationsSnapshotPtr getMutationsSnapshot() const { return mutations_snapshot; }

    const MergeTreeData & getMergeTreeData() const { return data; }
    const MergeTreeReaderSettings & getReaderSettings() const { return reader_settings; }
    size_t getMaxBlockSize() const { return block_size.max_block_size_rows; }
    size_t getNumStreams() const { return requested_num_streams; }
    bool isParallelReadingEnabled() const { return read_task_callback != std::nullopt; }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

    void setVectorSearchParameters(std::optional<VectorSearchParameters> && vector_search_parameters_) { vector_search_parameters = vector_search_parameters_; }
    std::optional<VectorSearchParameters> getVectorSearchParameters() const { return vector_search_parameters; }

    bool isParallelReadingFromReplicas() const { return is_parallel_reading_from_replicas; }
    void disableQueryConditionCache() { allow_query_condition_cache = false; }

    /// After projection optimization, ReadFromMergeTree may be replaced with a new reading step, and the ParallelReadingExtension must be forwarded to the new step.
    /// Meanwhile, the ParallelReadingExtension originally in ReadFromMergeTree might be clear.
    void clearParallelReadingExtension();
    std::shared_ptr<ParallelReadingExtension> getParallelReadingExtension();

    /// Announce an empty read set to the parallel-replicas coordinator (what initializePipeline() sends
    /// when there are no ranges). Callable from the projection optimizer when it replaces this step and
    /// initializePipeline() will not run. No-op unless this is the initiator local plan; returns whether
    /// an announcement was sent.
    bool announceEmptyReadRangesToCoordinatorIfInitiator();

    bool isParallelReplicasLocalPlanForInitiator() const;
    bool isParallelReplicasLocalPlanForFollower() const;

    /// Mark a (non-executed) read as a parallel-replicas read purely so that serialization records it.
    /// No callbacks are attached: the read is only serialized on the initiator and shipped to replicas,
    /// where deserialize rebuilds it in parallel-reading mode and resolves the callbacks from the context.
    void enableParallelReadingFromReplicasForSerialization() { is_parallel_reading_from_replicas = true; }

    bool supportsDataflowStatisticsCollection() const override { return !isQueryWithFinal(); }

    /// Adds virtual columns for reading from text index.
    /// Removes physical text columns that were eliminated by direct read from text index.
    void createReadTasksForTextIndex(const UsefulSkipIndexes & skip_indexes, const IndexReadColumns & added_columns, const Names & removed_columns, bool is_final);

    const std::optional<Indexes> & getIndexes() const { return indexes; }
    ConditionSelectivityEstimatorPtr getConditionSelectivityEstimator(const Names & required_columns) const;
    /// Compose statistics over the part set of the given partition/PK analysis result
    /// instead of all prepared parts. Passing nullptr falls back to getParts().
    ConditionSelectivityEstimatorPtr getConditionSelectivityEstimator(const Names & required_columns, const AnalysisResultPtr & analyzed_result) const;

    static void buildIndexes(
        std::optional<ReadFromMergeTree::Indexes> & indexes,
        const ActionsDAG * filter_actions_dag_,
        const MergeTreeData & data,
        const RangesInDataParts & parts,
        [[maybe_unused]] const std::optional<VectorSearchParameters> & vector_search_parameters,
        [[maybe_unused]] std::optional<TopKFilterInfo> top_k_filter_info,
        const ContextPtr & query_context,
        const SelectQueryInfo & query_info_,
        const StorageMetadataPtr & metadata_snapshot,
        bool skip_partition_pruning_ = false);

    void setTopKColumn(const TopKFilterInfo & top_k_filter_info_);
    bool isSkipIndexAvailableForTopK(const String & sort_column) const;
    const ProjectionIndexReadDescription & getProjectionIndexReadDescription() const { return projection_index_read_desc; }
    ProjectionIndexReadDescription & getProjectionIndexReadDescription() { return projection_index_read_desc; }
    /// In distributed query plan, this step will be executed in a distributed manner - shards will be read in parallel.
    void setDistributedRead(size_t bucket_count);
    /// Ceiling for the tasks of one distributed read (lanes per task are unbounded).
    static constexpr size_t max_distributed_read_buckets = 256;

    /// Splits the analyzed marks into up to `target_buckets` distributed-read buckets: mark-balanced
    /// slices for a plain read, primary-key-range layers grouped into the buckets for `FINAL`. Returns
    /// the bucket count, or 0 (read serially) when a `FINAL` read cannot be split safely.
    size_t setupDistributedReadBuckets(size_t target_buckets, size_t max_total_buckets);
    /// Serializes each bucket (its marks, the merge flag, and a merge layer's borders + index) into a
    /// per-bucket blob shipped as the per-read bucket task parameter; empty unless this is a distributed read.
    std::vector<String> serializeDistributedReadBuckets() const;
    /// Makes a list of shards to read in parallel in distributed query plan
    Strings getShardsForDistributedRead() const;

    bool canRemoveUnusedColumns() const override;
    RemoveUnusedColumnsResult removeUnusedColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) override;
    bool canRemoveColumnsFromOutput() const override;

    bool isSelectedForTopKFilterOptimization() const { return top_k_filter_info.has_value(); }
    const std::optional<TopKFilterInfo> & getTopKFilterInfo() const { return top_k_filter_info; }

    /// Carries the TopK stamp and the query condition cache gate over from a read step that this
    /// step replaces (e.g. the projection read built by `optimizeUseNormalProjections`; `clone` and
    /// `createLocalParallelReplicasReadingStep` do the same for the steps they rebuild internally).
    /// `condition_hash` already has the part-set salt folded in by `setTopKColumn`, and the gate has
    /// already been derived from the settings there, so both are copied as is; calling
    /// `setTopKColumn` again would fold the part-set salt in twice.
    void copyTopKFilterInfoAndQueryConditionCacheGate(const ReadFromMergeTree & replaced_step)
    {
        top_k_filter_info = replaced_step.top_k_filter_info;
        allow_query_condition_cache = replaced_step.allow_query_condition_cache;
    }

    std::unique_ptr<LazilyReadFromMergeTree> keepOnlyRequiredColumnsAndCreateLazyReadStep(const NameSet & required_outputs);
    void addStartingPartOffsetAndPartOffset(bool & added_part_starting_offset, bool & added_part_offset);

    void setLazyMaterializingRows(LazyMaterializingRowsPtr lazy_materializing_rows_) { lazy_materializing_rows = std::move(lazy_materializing_rows_); }

    void deferFiltersAfterFinalIfNeeded();

    /// Whether PREWHERE (present or moved from WHERE later) is applied after FINAL instead of during reading
    bool isPrewhereDeferredAfterFinal() const;

    const FilterDAGInfoPtr & getDeferredRowLevelFilter() const { return deferred_row_level_filter; }
    const PrewhereInfoPtr & getDeferredPrewhereInfo() const { return deferred_prewhere_info; }
    size_t getDistributedReadBucketCount() const { return distributed_read_bucket_count; }
    /// The task-parameter key under which this read's bucket marks travel. Unique per read so several
    /// bucketed reads can share one worker fragment (e.g. a broadcast join's partitioned probe side and
    /// its replicated build side) without their bucket blobs colliding in the shared parameter map.
    const String & getDistributedReadParamName() const { return distributed_read_param_name; }
    void setDistributedReadParamName(String param_name) { distributed_read_param_name = std::move(param_name); }
    bool getEnableVerticalFinal() const { return enable_vertical_final; }

    /// Whether a FINAL read must merge parts within each partition independently instead of globally
    /// (the `do_not_merge_across_partitions_select_final` rule, which may also be decided automatically).
    bool doNotMergePartsAcrossPartitionsFinal() const;

    /// The computed SAMPLE filter (predicate over the sampling key) when this read uses sampling, else empty.
    /// A distributed read ships it to the worker, which reapplies it after reading parts like the local path.
    std::optional<FilterDAGInfo> getSamplingFilter() const;

    /// Throws if this is a bucketed distributed read using a feature it cannot reproduce from pinned
    /// marks (read-in-order, deferred FINAL filters, a projection, or direct text index tasks).
    void verifyBucketedReadSupported() const;

    /// Whether this read can be split into distributed read buckets. False when it uses a feature a
    /// bucketed read cannot reproduce on the receiving node (read-in-order, deferred FINAL filters,
    /// a projection, or direct text index tasks); such a read stays serial.
    bool supportsBucketedRead() const;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }
    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    /// Cascades cross-group identity. This is the step where a wrong cross-group merge returns wrong
    /// rows - two reads of the same table with different pruning would collapse into one - so every
    /// member of `ReadFromMergeTree`, `SourceStepWithFilter`, `SourceStepWithFilterBase` and
    /// `SelectQueryInfo` is classified below, and every uncertainty is resolved fail-closed.
    ///
    /// Some state has no serialization at all (`KeyCondition`, `PartitionPruner`, part snapshots,
    /// settings snapshots, the query tree). Those fields get a **provenance witness**: the address of
    /// an object the step owns through a `shared_ptr`. Equal address means literally the same object,
    /// so equal content; a different address makes the two steps unequal even when their contents
    /// match, which costs a deduplication but never produces a wrong one. The address cannot be
    /// recycled behind our back: the owning `shared_ptr` keeps the object alive for as long as the
    /// step lives, `GroupExpression` pins the step a cached hash was computed from, and
    /// `cascadesIdentityEncodingsEqual` re-encodes both steps while both are alive.
    ///
    /// The pruning inputs were traced through `selectRangesToRead` (the member and the static
    /// overload), `buildIndexes`, `applyFilters` and `initializePipeline`; the trace is in the
    /// per-field entries below.
    ///
    /// Own fields:
    ///  - `data` - the database and table name are on the wire; the table UUID is **extras** so two
    ///    same-named tables (a re-created or exchanged table) cannot collide. `data.merging_params`
    ///    and the rest of the storage object are properties of that table, hence covered by the UUID.
    ///  - `data_settings` - **extras** (witness). A `MergeTreeSettings` snapshot; the static
    ///    `selectRangesToRead` reads `distributed_index_analysis_min_*_to_activate` from it and
    ///    `buildIndexes` reads the minmax-column settings, so it selects parts.
    ///  - `prepared_parts` - **extras** (witness). The part set to analyze (`getParts()`), not on the
    ///    wire: a worker re-snapshots the table. Two reads of one table can legitimately carry
    ///    different part lists (a projection part set, the primary-key-layer split of
    ///    `optimizeJoinByShards`), so this is a first-class pruning carrier.
    ///  - `mutations_snapshot` - **extras** (witness). Feeds `filterPartsByStatistics`,
    ///    `filterPartsByQueryConditionCache` and the patch-part/delete-bitmap application, i.e. which
    ///    rows a part yields.
    ///  - `all_column_names` - on the wire.
    ///  - `reader_settings` - excluded: derived. Built in the constructor by
    ///    `MergeTreeReaderSettings::createForQuery`, which is `createFromContext(context)` plus
    ///    `read_in_order = query_info.input_order_info != nullptr`; the context is witnessed and
    ///    `input_order_info` is in the extras (`requestReadingInOrder` sets both together). Its later
    ///    mutations (`force_read_complete_granules`, `use_query_condition_cache`) all happen inside
    ///    `initializePipeline`, i.e. after optimization.
    ///  - `actions_settings` - excluded: `ExpressionActionsSettings(context)`, derived from the
    ///    witnessed context.
    ///  - `block_size` - `max_block_size_rows` is on the wire; the other members are **extras**. They
    ///    are constructor-derived from the context, but the struct is `const` and cheap to encode, so
    ///    encode rather than argue.
    ///  - `result_sort_description` - **extras**. Derived from `input_order_info` and the sorting key
    ///    by `updateSortDescription`, but it is what `getSortDescription()` returns, which the
    ///    optimizer reads as a physical property.
    ///  - `requested_num_streams` - on the wire.
    ///  - `output_streams_limit` - **extras**: bounds the stream count and is what
    ///    `requestReadingInOrder` collapses `requested_num_streams` to.
    ///  - `output_each_partition_through_separate_port` - **extras**: it changes the number of output
    ///    ports, which the aggregation above consumes positionally.
    ///  - `max_block_numbers_to_read` - **extras** (witness). A pinned per-partition block-number
    ///    boundary (`select_sequential_consistency`); `filterPartsByPartition` drops parts above it.
    ///  - `indexes` - **extras** (witness of `key_condition` + the four `use_skip_indexes*` flags).
    ///    The memoized index-analysis state: key/minmax/part-offset conditions, the partition pruner,
    ///    the useful skip indexes and `part_values`. It has no serialization, and it is *not*
    ///    reproducible from the encoded fields: `applyFilters` builds it from the deferred-FINAL-
    ///    filtered DAG (`index_filter_dag_without_deferred`, which the step does not keep) and from
    ///    `skip_partition_pruning`, whereas the static `selectRangesToRead` would rebuild it from
    ///    `query_info.filter_actions_dag` with partition pruning enabled. `buildIndexes` allocates a
    ///    fresh `ConditionTemplate` for `key_condition` on every call and `Indexes` is copied as a
    ///    whole, so an equal `key_condition` address proves both `Indexes` values came from one
    ///    `buildIndexes` call and are therefore content-equal. `use_skip_indexes_on_data_read` is
    ///    assigned after the build (in `selectRangesToRead`), so all four flags are encoded
    ///    explicitly. Read-time pruning depends on this state directly: `initializePipeline` hands
    ///    `indexes->skip_indexes` and `indexes->key_condition_rpn_template` to
    ///    `MergeTreeSkipIndexReader`.
    ///  - `join_runtime_filters_for_index_analysis` - **extras** (content: count, then each
    ///    descriptor's `filter_id`, `key_column_name` and key type name, in container order).
    ///    Deliberately not serialized - a worker skips this pruning - but locally it prunes granules
    ///    through `buildRuntimeRangePredicate` and the dynamic skip-index filter.
    ///  - `deferred_row_level_filter`, `deferred_prewhere_info` - **extras** (content: the DAG, the
    ///    column name and the flags). Deferral both removes the filter from index analysis and moves
    ///    its application to after the FINAL merge, so it changes rows. The objects are *not* the wire
    ///    ones after a `clone`: `clone` copies these pointers but deep-copies
    ///    `query_info.row_level_filter` / `prewhere_info`, so the two can diverge and the content is
    ///    encoded rather than assumed equal to the wire copy.
    ///  - `skip_partition_pruning` - **extras**: `buildIndexes` turns it into
    ///    `PartitionPruner::skip_analysis` and into `skip_constant_folding`.
    ///  - `log` - excluded: logging only.
    ///  - `selected_parts`, `selected_rows`, `selected_marks` - excluded: introspection counters
    ///    copied out of the analysis result in `initializePipeline`.
    ///  - `query_task_size_limit` - **extras**: read-in-order task sizing, set by
    ///    `requestReadingInOrder` next to `input_order_info`.
    ///  - `vector_search_parameters` - **extras** (content). Selects the vector-similarity index
    ///    condition in `buildIndexes` and gates the query-condition cache.
    ///  - `analyzed_result_ptr` - **extras** (witness). When set it *is* the read set: `getParts()`
    ///    and `getAnalysisResult()` return it instead of re-analyzing, and `setAnalyzedResult` lets
    ///    another pass pin an analysis produced elsewhere (projections, parallel replicas). The
    ///    content (parts, mark ranges, sampling, read type) is intentionally not encoded: a witness is
    ///    sound, and encoding thousands of parts on every hash pass is not affordable. Note `clone`
    ///    deep-copies the analysis, so a clone and its original are unequal here - which is correct
    ///    anyway, since `clone` also drops `indexes`.
    ///  - `shared_virtual_fields` - excluded: filled in `initializePipeline` from the analysis result
    ///    and the storage id, i.e. after optimization, and derived from encoded state.
    ///  - `index_read_tasks` - **predicate-gated** (false when non-empty). Direct text-index reads
    ///    materialize `__text_index_*` virtual columns and pin the marks they read; the tasks have no
    ///    serialization.
    ///  - `is_parallel_reading_from_replicas` - on the wire as a flag, but **predicate-gated**: the
    ///    coordinator callbacks and the replica number that go with it are not on the wire and have no
    ///    encodable state, so a parallel-replicas read never opts in.
    ///  - `all_ranges_callback`, `read_task_callback`, `number_of_current_replica` -
    ///    **predicate-gated** with the flag above.
    ///  - `enable_vertical_final` - **extras**: chooses the FINAL implementation and is cleared by
    ///    `requestReadingInOrder`.
    ///  - `allow_query_condition_cache` - **extras**: it decides whether index analysis may skip
    ///    granules recorded by other queries, and it is copied around by
    ///    `copyTopKFilterInfoAndQueryConditionCacheGate`.
    ///  - `lazy_materializing_rows` - **extras** (witness): lazy row materialization state, not
    ///    carried by `clone`.
    ///  - `virtual_row_conversion` - **extras** (witness): the virtual row a read-in-order merge
    ///    announces; `requestReadingInOrder` can reset it.
    ///  - `top_k_filter_info` - **extras** (content, including the threshold-tracker witness). It
    ///    selects the TopK skip index, salts the query-condition-cache key, and the tracker is the
    ///    running threshold that prunes granules.
    ///  - `projection_index_read_desc` - **predicate-gated** (false when non-empty): per-projection
    ///    pinned read ranges with no serialization.
    ///  - `distributed_read_bucket_count`, `distributed_read_param_name` - on the wire, but the count
    ///    is **predicate-gated** to zero: the marks of each bucket live in `distributed_read_buckets`
    ///    / `distributed_read_task_buckets`, which are not on the wire and pin exactly what is read.
    ///    Gating the count to zero also covers all remaining throw sites of `serialize`
    ///    (`verifyBucketedReadSupported`, the bucketed deferred-FINAL rejection and the serialization
    ///    version check), and costs nothing: bucketing happens after the Cascades pass.
    ///  - `distributed_read_buckets`, `distributed_read_lanes_per_task`,
    ///    `distributed_read_task_buckets` - **predicate-gated** as above.
    ///
    /// Inherited from `SourceStepWithFilter` / `SourceStepWithFilterBase`:
    ///  - `filter_actions_dag` - **extras** (content, `addDAG`). The step-level pruning predicate. Not
    ///    on the wire, and a separate carrier from the `query_info` one: `applyFilters` copies it into
    ///    `query_info` only when it is non-null, so a null step DAG leaves whatever `query_info` had.
    ///  - `query_info.filter_actions_dag` - **extras** (content, `addDAG`), its own framed component.
    ///    This is the DAG index analysis actually reads (`buildIndexes`, the distributed index
    ///    analysis, the query-condition-cache key), so both DAGs are encoded, absent slots explicit.
    ///  - `filter_nodes`, `filter_dags` - **predicate-gated** through `hasPendingFilters()`: filters
    ///    still waiting for `applyFilters` are private to the base and would change the pruning.
    ///  - `limit` - **extras**: a trivial-limit bound on how much is read.
    ///  - `required_source_columns` - **extras**: the requested columns (`all_column_names` on the
    ///    wire is the read list, which the vector-search and lazy-read rewrites change independently).
    ///  - `query_info` - not on the wire as a whole. Members that the MergeTree read path reads
    ///    (verified by enumerating every `query_info.` use in `ReadFromMergeTree.cpp` and
    ///    `MergeTreeDataSelectExecutor.cpp`): `prewhere_info` and `row_level_filter` are on the wire;
    ///    `table_expression_modifiers` is on the wire except its `stream_settings` (predicate-gated
    ///    via `isStream()`); `filter_actions_dag`, `input_order_info`, `trivial_limit` and
    ///    `is_internal` are **extras**; `isFinal()` is **extras** as a bool, because with no
    ///    modifiers it falls back to `apply_query_level_final_if_no_modifiers` and the query-level
    ///    FINAL, neither of which is on the wire. Every pointer-valued member (`query`, `view_query`,
    ///    `query_tree`, `planner_context`, `table_expression`, `storage_limits`,
    ///    `initial_storage_snapshot`, `cluster`, `optimized_cluster`, `syntax_analyzer_result`,
    ///    `additional_filter_ast`, `filter_asts`, `order_optimizer`, `prepared_sets`) goes into one
    ///    framed provenance-witness component: `query`/`query_tree` decide sampling and
    ///    `supportsSkipIndexesOnDataRead`, `storage_limits` bounds what may be read, `prepared_sets`
    ///    holds the IN sets index analysis uses, and the rest is witnessed rather than argued about.
    ///    The remaining scalars (`local_storage_limits`, `has_window`, `has_order_by`,
    ///    `need_aggregate`, `has_aggregates`, `settings_limit_offset_done`, `is_parameterized_view`,
    ///    `optimize_trivial_count`, `columns_mask`) are excluded: no MergeTree read path reads them.
    ///  - `storage_snapshot` - **extras** (witness): the metadata snapshot (primary key, partition
    ///    key, skip indexes, virtuals) and the part snapshot every analysis step reads.
    ///  - `context` - **extras** (witness): the settings that gate partition pruning, skip indexes,
    ///    the query-condition cache, distributed index analysis and the runtime-filter lookup.
    ///
    /// Inherited from `ISourceStep` / `IQueryPlanStep`:
    ///  - `output_header` - covered by the identity encoding itself.
    ///  - `input_headers` - empty for a source step.
    ///  - `step_description`, `step_index`, `processors`, `dataflow_cache_updater` - display or
    ///    instrumentation only, excluded.
    ///
    /// `isSerializable()` is unconditionally true. `serializeSettings` is not overridden anywhere in
    /// this hierarchy, so it writes nothing and cannot throw. `serialize` throws for a STREAM read and
    /// for the bucketed cases, both predicate-gated; the DAGs it writes throw on a correlated
    /// `PLACEHOLDER` node, so the predicate checks `hasCorrelatedColumns` on every DAG the encoding
    /// writes, not only on the one `hasCorrelatedExpressions()` looks at.
    bool supportsCascadesIdentity() const override;
    void appendCascadesIdentityExtras(CascadesIdentityExtras & extras) const override;

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

    /// Used for granule pruning in JOINs (enable_join_runtime_filters_index_analysis).
    /// Populated post-construction by addJoinRuntimeFilterIndexAnalysisOnDataRead during query-plan
    /// optimization. Not carried by clone()/serialize()/deserialize(), so the pruning is intentionally
    /// skipped when the step is rebuilt for distributed or parallel-replicas reads (results stay correct,
    /// only the optimization is lost); propagating it there is a follow-up.
    std::vector<RuntimeFilterIndexAnalysisDescriptor> join_runtime_filters_for_index_analysis;

    /// Row policy / prewhere deferred to after FINAL, if needed
    FilterDAGInfoPtr deferred_row_level_filter;
    PrewhereInfoPtr deferred_prewhere_info;
    bool skip_partition_pruning = false;

    LoggerPtr log;
    UInt64 selected_parts = 0;
    UInt64 selected_rows = 0;
    UInt64 selected_marks = 0;

    /// When query has WHERE and LIMIT we cannot stop reading after reaching the limit,
    /// because we can read many rows that do not satisfy the condition.
    /// But we still use this estimation to get smaller task size for reading in order
    /// in case filter is not selective and to avoid reading too many rows in first task.
    UInt64 query_task_size_limit = 0;

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
        UInt64 limit,
        /// Index of this split when reading in-order with parallel replicas; nullopt means
        /// a single pool reads the whole table (no splitting).
        std::optional<size_t> split_index = std::nullopt);

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

    Pipe groupPartitionsByStreams(AnalysisResult & result);

    Pipe readByLayers(
        const RangesInDataParts & parts_with_ranges,
        SplitPartsByRanges split_parts,
        const MergeTreeIndexBuildContextPtr & index_build_context,
        const Names & column_names,
        const InputOrderInfoPtr & input_order_info);

    /// A pipe of `num_streams` `NullSource`s, used when there is nothing to read.
    Pipe createEmptyPipe(size_t num_streams) const;

    /// How many output ports this step must produce when there is nothing to read. Normally one, but
    /// when the parts are pre-split into primary-key layers by `optimizeJoinByShards`, the number of
    /// ports is a part of the plan: the JOIN above consumes exactly one port per layer and pairs the
    /// ports of its two sides positionally (see `QueryPipelineBuilder::joinPipelinesYShapedByShards`).
    size_t getNumStreamsWhenNothingToRead(const AnalysisResult & result) const;

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

    bool isRowPolicyDeferredAfterFinal() const;

    Pipe spreadMarkRangesAmongStreamsFinal(
        RangesInDataParts && parts,
        const MergeTreeIndexBuildContextPtr & index_build_context,
        size_t num_streams,
        const Names & origin_column_names,
        const Names & column_names,
        std::optional<ActionsDAG> & out_projection);

    /// Reads non-intersecting primary-key ranges (each owned by a single deduplicated part) without a
    /// merge, applying only the filter the merge would have applied (drop negative-sign rows for
    /// `Collapsing`, `is_deleted` rows for `Replacing` with an is-deleted column; other engines need none).
    Pipe readNonIntersectingWithEngineFilter(
        RangesInDataParts && parts,
        const MergeTreeIndexBuildContextPtr & index_build_context,
        size_t num_streams,
        const Names & origin_column_names);

    ReadFromMergeTree::AnalysisResult & getAnalysisResultImpl() const;
    const ReadFromMergeTree::AnalysisResult & getAnalysisResult() const { return getAnalysisResultImpl(); }
    ReadFromMergeTree::AnalysisResult & getAnalysisResult() { return getAnalysisResultImpl(); }

    void logPredicateStatistics(const AnalysisResult & result) const;

    int getSortDirection() const;
    void updateSortDescription();

    bool supportsSkipIndexesOnDataRead() const;

    mutable AnalysisResultPtr analyzed_result_ptr;
    VirtualFields shared_virtual_fields;
    IndexReadTasks index_read_tasks;

    bool is_parallel_reading_from_replicas;
    std::optional<MergeTreeAllRangesCallback> all_ranges_callback;
    std::optional<MergeTreeReadTaskCallback> read_task_callback;
    bool enable_vertical_final = false;
    bool allow_query_condition_cache = true;

    LazyMaterializingRowsPtr lazy_materializing_rows;

    ExpressionActionsPtr virtual_row_conversion;

    std::optional<size_t> number_of_current_replica;

    std::optional<TopKFilterInfo> top_k_filter_info;
    ProjectionIndexReadDescription projection_index_read_desc;
    /// Number of tasks when this leaf read is distributed; each worker reads the lanes described by its
    /// per-read bucket parameter.
    size_t distributed_read_bucket_count = 0;
    /// Per-read task-parameter key for this read's bucket marks (see getDistributedReadParamName).
    String distributed_read_param_name;
    /// Initiator side: every virtual bucket across all tasks; `serializeDistributedReadBuckets` groups
    /// `distributed_read_lanes_per_task` of them into each task's bucket parameter. Empty on a worker.
    std::vector<DistributedReadBucket> distributed_read_buckets;
    size_t distributed_read_lanes_per_task = 1;
    /// Worker side: the virtual buckets (lanes) of this worker's task, filled from its bucket
    /// parameter. A FINAL worker builds one merge/non-merge pipe per lane and unites them.
    std::vector<DistributedReadBucket> distributed_read_task_buckets;
};

}
