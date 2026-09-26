#pragma once

#include <AggregateFunctions/TimeSeries/ITimeSeriesRateToGridStreaming.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Transforms/PromQLRangeRateTransform.h>
#include <Processors/Transforms/PromQLTwoRangeRatesTransform.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Core/SortCursor.h>
#include <Core/SortDescription.h>
#include <Common/AlignedBuffer.h>

#include <optional>
#include <vector>


namespace DB
{

/// Merges ordered `(id, bucket)` streams and feeds rows directly into the
/// single-series native PromQL range-rate state. Unlike `MergingSortedAlgorithm`,
/// this algorithm never materializes a merged `Samples` array.
class PromQLRangeRateMergingAlgorithm final : public IMergingAlgorithm
{
public:
    using Collector = PromQLRangeRateTransform::Collector;
    using CollectorPtr = PromQLRangeRateTransform::CollectorPtr;
    using Group = PromQLRangeRateTransform::Group;

    PromQLRangeRateMergingAlgorithm(
        SharedHeader header_,
        size_t num_inputs,
        CollectorPtr collector_,
        AggregateFunctionPtr rate_function_,
        size_t max_samples_per_series_,
        size_t max_output_block_size_,
        std::optional<Field> raw_min_time_ = {},
        std::optional<Field> raw_max_time_ = {},
        PromQLTwoRangeRatesSeriesMatcherPtr series_matcher_ = nullptr);

    ~PromQLRangeRateMergingAlgorithm() override;

    const char * getName() const override { return "PromQLRangeRateMergingAlgorithm"; }

    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

    MergedStats getMergedStats() const override { return merged_stats; }

private:
    struct SourceState
    {
        ColumnPtr samples_column;
        const ColumnArray * samples_array = nullptr;
        const ColumnTuple * raw_samples_tuple = nullptr;
        PaddedPODArray<Group> full_groups;
    };

    SharedHeader header;
    CollectorPtr collector;
    AggregateFunctionPtr rate_function;
    const size_t max_samples_per_series;
    const size_t max_output_block_size;
    size_t id_position = 0;
    size_t bucket_position = 0;
    size_t samples_position;
    const bool reads_raw_samples;

    AlignedBuffer rate_place;
    MutableColumnPtr current_id;
    MutableColumnPtr current_bucket;
    MutableColumnPtr last_input_id;
    MutableColumnPtr last_input_bucket;
    MutableColumnPtr raw_min_time_column;
    MutableColumnPtr raw_max_time_column;

    Group current_output_group = Collector::getGroupForNoTags();
    size_t current_series_samples = 0;
    bool has_current_series = false;
    bool rate_state_created = false;
    bool has_current_external_bucket = false;

    const ITimeSeriesRateToGridStreaming * streaming_rate = nullptr;
    ITimeSeriesRateToGridStreaming::StatePtr streaming_rate_state;

    std::vector<IMergingAlgorithm::Input> current_inputs;
    std::vector<SourceState> source_states;
    SortCursorImpls cursors;
    SortQueueVariants queue_variants;
    SortDescription description;
    MergedStats merged_stats;
    PromQLRangeRateGroupSetPtr output_groups;
    PromQLTwoRangeRatesSeriesMatcherPtr series_matcher;
    MutableColumnPtr rate_result;
    MutableColumnPtr output_group_column;
    MutableColumnPtr output_values_column;
    Group current_full_group = Collector::getGroupForNoTags();

    void initializeSource(size_t source_num);
    void rejectUnsupportedInput(const Input & input) const;
    void rejectUnsupportedChunk(const Chunk & chunk) const;
    bool consumeRow(
        size_t source_num,
        size_t row,
        MutableColumnPtr & group_column,
        MutableColumnPtr & values_column);
    void startSeries(const IColumn & id_column, size_t row, Group full_group);
    void finishExternalBucket();
    void finishSeries(MutableColumnPtr & group_column, MutableColumnPtr & values_column);
    void checkAndRememberInputOrder(const IColumn & id_column, const IColumn & bucket_column, size_t row);
    void destroyRateState() noexcept;

    template <typename SortingQueue>
    Status mergeBatchImpl(SortingQueue & queue);
};

}
