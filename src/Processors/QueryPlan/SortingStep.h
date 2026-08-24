#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/TopKThresholdTracker.h>
#include <Core/SortDescription.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/TemporaryDataOnDisk.h>

namespace DB
{

class QueryPipelineProcessorsCollector;

/// Sort data stream
class SortingStep : public ITransformingStep
{
public:

    enum class SortingStage : uint8_t
    {
        Scatter = 0,
        Sort = 1,
        MergeStreams = 2,
        FinishSort = 3,
    };

    enum class Type : uint8_t
    {
        /// Performs a complete sorting operation and returns a single fully ordered data stream
        Full,

        /// Completes the sorting process for partially sorted data.
        FinishSorting,

        /// Applies FinishSorting for partitioned partially sorted data.
        /// The sorting is applied within each partition separately without merging them.
        PartitionedFinishSorting,

        /// Merges multiple sorted streams into a single sorted output.
        MergingSorted,
    };

    struct Settings
    {
        size_t max_block_size;
        SizeLimits size_limits;
        size_t max_bytes_before_remerge = 0;
        float remerge_lowered_memory_bytes_ratio = 0;

        double max_bytes_ratio_before_external_sort = 0.;
        size_t max_bytes_in_block_before_external_sort = 0;
        size_t max_bytes_in_query_before_external_sort = 0;

        size_t min_free_disk_space = 0;
        size_t max_block_bytes = 0;
        size_t read_in_order_use_buffering = 0;
        bool read_in_order_use_virtual_row_per_block = false;
        size_t temporary_files_buffer_size = 0;
        String temporary_files_codec = {};

        explicit Settings(const DB::Settings & settings);
        explicit Settings(size_t max_block_size_);
        explicit Settings(const QueryPlanSerializationSettings & settings);

        void updatePlanSettings(QueryPlanSerializationSettings & settings) const;

        bool operator==(const Settings & other) const = default;
    };

    /// Full
    SortingStep(
        const SharedHeader & input_header,
        SortDescription description_,
        UInt64 limit_,
        const Settings & settings_,
        bool is_sorting_for_merge_join_ = false);

    /// Full with partitioning
    SortingStep(
        const SharedHeader & input_header,
        const SortDescription & description_,
        const SortDescription & partition_by_description_,
        UInt64 limit_,
        const Settings & settings_);

    /// FinishSorting
    SortingStep(
        const SharedHeader & input_header,
        SortDescription prefix_description_,
        SortDescription result_description_,
        size_t max_block_size_,
        UInt64 limit_);

    /// MergingSorted
    SortingStep(
        const SharedHeader & input_header,
        SortDescription sort_description_,
        const Settings & settings_,
        UInt64 limit_ = 0,
        bool always_read_till_end_ = false);

    String getName() const override { return "Sorting"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    UInt64 getLimit() const { return limit; }
    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);

    const SortDescription & getSortDescription() const override { return result_description; }

    bool hasPartitions() const { return !partition_by_description.empty(); }
    const SortDescription & getPartitionByDescription() const { return partition_by_description; }
    Names getPartitionByColumnNames() const;

    size_t getScatterPartitions() const { return scatter_partitions; }

    /// Do not reshuffle the input by the hash of the partition columns before sorting: the input streams
    /// already carry disjoint sets of the partition key values, so sorting each stream independently is
    /// enough to keep every partition contiguous and sorted.
    void skipScatterByPartition() { skip_scatter_by_partition = true; }

    bool isSortingForMergeJoin() const { return is_sorting_for_merge_join; }

    bool isPartialTopN() const { return is_partial_top_n; }
    void setPartialTopN() { is_partial_top_n = true; }

    void convertToFinishSorting(SortDescription prefix_description, bool use_buffering_, bool apply_virtual_row_conversions_);

    void enableBuffering() { use_buffering = true; }
    bool getUseBuffering() const { return use_buffering; }

    Type getType() const { return type; }
    const Settings & getSettings() const { return sort_settings; }

    void convertToPartitionedFinishSorting() { type = Type::PartitionedFinishSorting; }

    /// Switch to a full sort that scatters the input by the hash of the sort key into exactly
    /// `partitions` independent partitions and sorts each partition separately, producing one sorted
    /// stream per partition (no final merge). Unlike the partition-by-window-frame scatter, the partition
    /// count is fixed (not the pipeline's thread count), so both sides of a join scatter into the same
    /// number of shards regardless of how many streams each side reads. Used by
    /// `parallel_full_sorting_merge` to feed a hash-sharded merge join.
    void convertToScatteredFullSort(size_t partitions)
    {
        partition_by_description = result_description;
        type = Type::Full;
        scatter_partitions = partitions;
    }

    static void fullSortStreams(
        QueryPipelineBuilder & pipeline,
        const Settings & sort_settings,
        const SortDescription & result_sort_desc,
        UInt64 limit_,
        bool skip_partial_sort = false,
        TopKThresholdTrackerPtr threshold_tracker = nullptr);

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    /// `scatter_partitions != 0` means a fixed-shard-count scatter (`convertToScatteredFullSort`, used by
    /// `parallel_full_sorting_merge`); `scatter_partitions` is not on the wire, so such a sort must stay
    /// unserializable rather than have a worker silently rebuild it as an ordinary partitioned sort.
    bool isSerializable() const override
    {
        return (type == Type::Full || type == Type::FinishSorting) && scatter_partitions == 0;
    }

    /// Cascades cross-group identity. Field audit of every member of `SortingStep`,
    /// `ITransformingStep` and `IQueryPlanStep`. `supportsCascadesIdentity()` implies
    /// `isSerializable()`, i.e. `type` is `Full` or `FinishSorting` and `scatter_partitions == 0`, so
    /// reachability below is checked against the two branches `transformPipeline` then takes:
    /// `fullSort` (`scatterByPartitionIfNeeded` + `fullSortStreams` + `addPerStreamLimitByIfNeeded` +
    /// a final `MergingSortedTransform`) and the `FinishSorting` branch (`addPerStreamLimitByIfNeeded`
    /// + `mergingSorted` + `finishSorting`).
    ///
    /// Own fields:
    ///  - `type` - on the wire (`serialize`, flags bit 1 distinguishes `FinishSorting` from `Full`);
    ///    the other two values are excluded by `isSerializable`.
    ///  - `result_description` - on the wire (`serialize`, unconditionally).
    ///  - `prefix_description` - on the wire for `FinishSorting`. For `Full` it is not written, and it
    ///    is also not read: `fullSort` never looks at it, and the only setters
    ///    (`convertToFinishSorting`, the `FinishSorting` constructor) leave `type != Full`.
    ///  - `partition_by_description` - on the wire (`serialize`, unconditionally).
    ///  - `scatter_partitions` - covered by the predicate: `isSerializable()` requires it to be 0.
    ///  - `skip_scatter_by_partition` - **extras**. Not on the wire. `scatterByPartitionIfNeeded`
    ///    returns immediately when set, so a partitioned full sort either reshuffles rows across
    ///    streams by the partition hash or does not - a different pipeline over the same input.
    ///  - `is_sorting_for_merge_join` - **extras**. Not on the wire. It is what
    ///    `optimizeJoinByShards`, `useDataParallelAggregation`, `applyParallelReplicas` and
    ///    `findParallelReplicasQuery` test to decide whether the sort may be resharded, parallelized
    ///    or sent to replicas, so two sorts differing only here are not interchangeable for the
    ///    optimizer.
    ///  - `is_partial_top_n` - **extras**. Deliberately not serialized (the executed sort is the
    ///    same), but load-bearing for identity: `Cascades/Cost.cpp` costs a partial top-N differently,
    ///    and `TwoStageTopN` produces its partial stage by cloning the sort and flipping only this
    ///    flag - without it in the identity the two stages would be judged equal and memo-wide
    ///    deduplication would fold the rule's output into a self-cycle.
    ///  - `limit` - on the wire (`serialize`, at query plan serialization version >= 8, which the
    ///    identity encoding always uses).
    ///  - `always_read_till_end` - **extras**. Not on the wire, and it is read on both serializable
    ///    branches: it is passed to the final `MergingSortedTransform` in `fullSort` and in
    ///    `mergingSorted`, where it decides whether exhausted-but-unneeded inputs are still drained.
    ///    Only the `MergingSorted` constructor sets it, but nothing keeps that instance from being
    ///    converted (`convertToFinishSorting`), so it is not derivable from `type`.
    ///  - `use_buffering` - on the wire where it is read: `serialize` writes it in flags bit 2 for
    ///    `FinishSorting`, and the only reader is `mergingSorted`, which the `Full` branch never
    ///    calls. `enableBuffering` can set it on a `Full` sort, where it is inert.
    ///  - `apply_virtual_row_conversions` - **extras**. On the wire for `FinishSorting` only (flags
    ///    bit 4), yet `fullSort` reads it too, adding a `RemoveVirtualRowTransform` when no final
    ///    merge is inserted. Included rather than argued away through `convertToScatteredFullSort`
    ///    being the only route to a `Full` sort that has it set.
    ///  - `threshold_tracker` - excluded: a runtime object shared with other steps by `optimizeTopK`
    ///    (`setTopKThresholdTracker`) that only publishes a top-N pruning threshold; it has no stable
    ///    value to encode, and the rows a sort produces do not depend on it. Safe in the reader's
    ///    direction too: deduping away a tracker-feeding sort just leaves the reader's tracker
    ///    without updates (less pruning, never wrong rows), and a content-equal replacement sort
    ///    publishes valid thresholds over the same input.
    ///  - `limit_by_columns`, `limit_by_group_length` - **extras**. Not on the wire. Set by
    ///    `pushLimitByIntoSort`; when the hint is a sort prefix, `addPerStreamLimitByIfNeeded`
    ///    installs a per-stream `LimitBySortedStreamTransform`, which drops rows.
    ///  - `scatter_stage`, `sorting_stage`, `merge_streams`, `finalizing` - runtime instrumentation
    ///    for `describePipeline`, excluded.
    ///
    /// `sort_settings` (`SortingStep::Settings`), field by field:
    ///  - `max_block_size`, `size_limits` (all three members), `max_bytes_before_remerge`,
    ///    `remerge_lowered_memory_bytes_ratio`, `max_bytes_ratio_before_external_sort`,
    ///    `max_bytes_in_block_before_external_sort`, `min_free_disk_space`, `max_block_bytes`,
    ///    `temporary_files_buffer_size`, `temporary_files_codec` - on the wire
    ///    (`Settings::updatePlanSettings`).
    ///  - `max_bytes_in_query_before_external_sort` - derived, excluded: both constructors compute it
    ///    from `max_bytes_ratio_before_external_sort`, which is on the wire, and the available system
    ///    memory, which is not a property of the step.
    ///  - `read_in_order_use_buffering`, `read_in_order_use_virtual_row_per_block` - **extras**.
    ///    `updatePlanSettings` writes neither (the deserializing constructor hardcodes
    ///    `read_in_order_use_buffering = false`), and `mergingSorted` - the `FinishSorting` branch -
    ///    reads both to decide whether to insert a `BufferChunksTransform` and whether virtual rows
    ///    are used per block.
    ///
    /// Inherited:
    ///  - `output_header` - covered by the identity encoding itself.
    ///  - `input_headers` - derived, excluded: `updateOutputHeader` copies the input header to the
    ///    output header, so the encoded output header is the input header.
    ///  - `transform_traits`, `data_stream_traits` - derived, excluded: computed by `getTraits` from
    ///    `limit` at construction; `updateLimit` recomputes `preserves_number_of_rows` from `limit`,
    ///    which is on the wire.
    ///  - `collect_processors` - derived, excluded: false exactly for the `MergingSorted`
    ///    constructor, which `isSerializable` excludes.
    ///  - `step_description`, `step_index`, `processors`, `dataflow_cache_updater` - display or
    ///    runtime instrumentation only, excluded.
    ///
    /// `serialize` throws for a non-`Full`/`FinishSorting` type (excluded by `isSerializable`) and for
    /// query plan serialization versions below 6, or below 8 with a non-zero `limit`; the identity
    /// encoding always uses `DBMS_QUERY_PLAN_SERIALIZATION_VERSION`, which is above both. The step
    /// holds no `ActionsDAG`, so no correlated-expression guard is needed.
    ///
    /// `serializeSettings` can throw too, and `isSerializable()` does not cover it: the
    /// `Settings(size_t max_block_size_)` constructor leaves `temporary_files_buffer_size` at 0, and
    /// the plan setting of that name is a `NonZeroUInt64`, so assigning it throws `BAD_ARGUMENTS`.
    /// `optimizeGroupByTopK` builds a real `Full` sort that way, so the predicate must exclude it.
    bool supportsCascadesIdentity() const override { return isSerializable() && sort_settings.temporary_files_buffer_size != 0; }
    void appendCascadesIdentityExtras(CascadesIdentityExtras & extras) const override;

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    bool supportsDataflowStatisticsCollection() const override { return true; }
    void setTopKThresholdTracker(TopKThresholdTrackerPtr threshold_tracker_) { threshold_tracker = threshold_tracker_; }

    void updateLimitByHint(Names limit_by_columns_, UInt64 limit_by_group_length_);

    std::vector<size_t> getStepGroups() const override;
    String getStepGroupName(size_t group) const override;

    void describePipeline(FormatSettings & settings) const override;

private:
    void scatterByPartitionIfNeeded(QueryPipelineBuilder& pipeline);
    void updateOutputHeader() override;

    /// Adds a per-stream `LimitByTransform` before sorted streams are merged into one.
    /// This reduces rows processed by the final merge and later pipeline steps.
    /// It is applied only when `LIMIT BY` keys are a prefix of `stream_sort_desc`.
    void addPerStreamLimitByIfNeeded(QueryPipelineBuilder & pipeline, const SortDescription & stream_sort_desc);

    static void mergeSorting(
        QueryPipelineBuilder & pipeline,
        const Settings & sort_settings,
        const SortDescription & result_sort_desc,
        UInt64 limit_, TopKThresholdTrackerPtr threshold_tracker);

    void mergingSorted(
        QueryPipelineBuilder & pipeline,
        const SortDescription & result_sort_desc,
        UInt64 limit_);
    void finishSorting(
        QueryPipelineBuilder & pipeline,
        const SortDescription & input_sort_desc,
        const SortDescription & result_sort_desc,
        UInt64 limit_);
    void fullSort(
        QueryPipelineBuilder & pipeline,
        const SortDescription & result_sort_desc,
        UInt64 limit_,
        QueryPipelineProcessorsCollector & collector,
        bool skip_partial_sort = false);

    Type type;

    SortDescription prefix_description;
    const SortDescription result_description;

    SortDescription partition_by_description;
    /// When > 0, `scatterByPartitionIfNeeded` scatters into exactly this many partitions (instead of the
    /// pipeline's thread count), so both sides of a hash-sharded merge join get the same shard count.
    size_t scatter_partitions = 0;
    bool skip_scatter_by_partition = false;

    /// See `findQueryForParallelReplicas`
    bool is_sorting_for_merge_join = false;

    /// A distributed plan can split a top-N sort in two stages: each node keeps its local
    /// top `limit` rows, and a limit above keeps the global top `limit` of the merged result.
    /// This flag marks the first stage. It only tells the optimizer what the sort is for (like
    /// `is_sorting_for_merge_join`); the executed sort is the same, so it is not serialized.
    bool is_partial_top_n = false;

    UInt64 limit;
    bool always_read_till_end = false;
    bool use_buffering = false;
    bool apply_virtual_row_conversions = false;

    TopKThresholdTrackerPtr threshold_tracker;

    Settings sort_settings;

    /// See `pushLimitByIntoSort`. Empty means no hint.
    Names limit_by_columns;
    UInt64 limit_by_group_length = 0;

    Processors scatter_stage;
    Processors sorting_stage;
    Processors merge_streams;
    Processors finalizing;

};

}
