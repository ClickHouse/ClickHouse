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
        const Settings & settings_,
        UInt64 limit_);

    /// MergingSorted
    SortingStep(
        const SharedHeader & input_header,
        SortDescription sort_description_,
        const Settings & settings_,
        UInt64 limit_ = 0,
        bool always_read_till_end_ = false);

    [[nodiscard]] String getName() const override { return "Sorting"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    [[nodiscard]] UInt64 getLimit() const { return limit; }
    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);

    [[nodiscard]] const SortDescription & getSortDescription() const override { return result_description; }

    [[nodiscard]] bool hasPartitions() const { return !partition_by_description.empty(); }
    [[nodiscard]] const SortDescription & getPartitionByDescription() const { return partition_by_description; }
    [[nodiscard]] Names getPartitionByColumnNames() const;

    [[nodiscard]] size_t getScatterPartitions() const { return scatter_partitions; }

    /// Do not reshuffle the input by the hash of the partition columns before sorting: the input streams
    /// already carry disjoint sets of the partition key values, so sorting each stream independently is
    /// enough to keep every partition contiguous and sorted.
    void skipScatterByPartition() { skip_scatter_by_partition = true; }

    [[nodiscard]] bool isSortingForMergeJoin() const { return is_sorting_for_merge_join; }

    [[nodiscard]] bool isPartialTopN() const { return is_partial_top_n; }
    void setPartialTopN() { is_partial_top_n = true; }

    void convertToFinishSorting(SortDescription prefix_description, bool use_buffering_, bool apply_virtual_row_conversions_);

    void enableBuffering() { use_buffering = true; }
    [[nodiscard]] bool getUseBuffering() const { return use_buffering; }

    [[nodiscard]] Type getType() const { return type; }
    [[nodiscard]] const Settings & getSettings() const { return sort_settings; }

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

    /// Sorts the main streams, or only the totals stream if `sort_totals` is set.
    static void fullSortStreams(
        QueryPipelineBuilder & pipeline,
        const Settings & sort_settings,
        const SortDescription & result_sort_desc,
        UInt64 limit_,
        bool skip_partial_sort = false,
        TopKThresholdTrackerPtr threshold_tracker = nullptr,
        bool sort_totals = false);

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    /// `scatter_partitions != 0` means a fixed-shard-count scatter (`convertToScatteredFullSort`, used by
    /// `parallel_full_sorting_merge`); `scatter_partitions` is not on the wire, so such a sort must stay
    /// unserializable rather than have a worker silently rebuild it as an ordinary partitioned sort.
    [[nodiscard]] bool isSerializable() const override
    {
        return (type == Type::Full || type == Type::FinishSorting) && scatter_partitions == 0;
    }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    [[nodiscard]] QueryPlanStepPtr clone() const override;

    [[nodiscard]] bool hasCorrelatedExpressions() const override { return false; }

    [[nodiscard]] bool supportsDataflowStatisticsCollection() const override { return true; }
    void setTopKThresholdTracker(TopKThresholdTrackerPtr threshold_tracker_) { threshold_tracker = threshold_tracker_; }

    void updateLimitByHint(Names limit_by_columns_, UInt64 limit_by_group_length_, bool limit_by_always_read_till_end_);

    [[nodiscard]] std::vector<size_t> getStepGroups() const override;
    [[nodiscard]] String getStepGroupName(size_t group) const override;

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
        UInt64 limit_, TopKThresholdTrackerPtr threshold_tracker, bool sort_totals);

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
    bool limit_by_always_read_till_end = false;

    Processors scatter_stage;
    Processors sorting_stage;
    Processors merge_streams;
    Processors finalizing;

};

}
