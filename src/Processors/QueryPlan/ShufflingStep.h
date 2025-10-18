#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/TemporaryDataOnDisk.h>

namespace DB
{

/// Shuffle data stream
class ShufflingStep : public ITransformingStep
{
public:
    enum class Type : uint8_t
    {
        /// Performs a complete shuffling operation and returns a single fully shuffled data stream
        Full,

        /// Merges multiple shuffled streams into a single shuffled output.
        MergingShuffled,
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

        explicit Settings(const DB::Settings & settings);
        explicit Settings(size_t max_block_size_);
        explicit Settings(const QueryPlanSerializationSettings & settings);

        void updatePlanSettings(QueryPlanSerializationSettings & settings) const;
    };

    /// Full
    ShufflingStep(
        const SharedHeader & input_header,
        UInt64 limit_,
        const Settings & settings_);

    /// MergingShuffled
    ShufflingStep(
        const SharedHeader & input_header,
        size_t max_block_size_,
        UInt64 limit_ = 0,
        bool always_read_till_end_ = false
    );

    String getName() const override { return "Shuffling"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    UInt64 getLimit() const { return limit; }
    /// Add limit or change it to lower value.
    // void updateLimit(size_t limit_);

    // bool hasPartitions() const { return !partition_by_description.empty(); }

    Type getType() const { return type; }
    const Settings & getSettings() const { return sort_settings; }

    static void fullShuffleStreams(
        QueryPipelineBuilder & pipeline,
        const Settings & sort_settings,
        UInt64 limit_,
        bool skip_partial_shuffle = false);

    void serializeSettings(QueryPlanSerializationSettings & settings) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    // void scatterByPartitionIfNeeded(QueryPipelineBuilder& pipeline);
    void updateOutputHeader() override;

    static void mergeShuffling(
        QueryPipelineBuilder & pipeline,
        const Settings & sort_settings,
        UInt64 limit_);

    void mergingShuffled(
        QueryPipelineBuilder & pipeline,
        UInt64 limit_);

    void fullShuffle(
        QueryPipelineBuilder & pipeline,
        UInt64 limit_,
        bool skip_partial_shuffle = false);

    Type type;

    /// See `findQueryForParallelReplicas`

    UInt64 limit;
    bool always_read_till_end = false;
    // bool use_buffering = false;
    // bool apply_virtual_row_conversions = false;

    Settings sort_settings;
};

}
