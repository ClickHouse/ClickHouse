#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/IColumn_fwd.h>
#include <Core/Field.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <Processors/IProcessor.h>
#include <Common/AlignedBuffer.h>
#include <Common/HashTable/HashMap.h>
#include <Common/PODArray.h>

#include <memory>
#include <mutex>
#include <optional>
#include <vector>


namespace DB
{

/// Query-wide bounded vector-matching state shared by parallel primary-key lanes.
/// The lock is acquired once per completed physical series, after the rate grid
/// has already been calculated by the lane-local aggregate state.
class PromQLTwoRangeRatesGroupState
{
public:
    using Collector = ContextTimeSeriesTagsCollector;
    using Group = Collector::Group;

    struct Match
    {
        Group group = Collector::getGroupForNoTags();
        ColumnPtr values;
    };

    PromQLTwoRangeRatesGroupState(size_t max_join_groups_, size_t max_grid_cells_);

    std::optional<Match> add(
        Group join_group,
        size_t side,
        const String & metric_name,
        MutableColumnPtr & rate_result,
        size_t grid_cells);

private:
    struct PendingGroup
    {
        Group group = Collector::getGroupForNoTags();
        UInt8 seen_sides = 0;
        ColumnPtr values;
        size_t grid_cells = 0;
    };

    using GroupIndexMap = HashMap<Group, UInt64, HashCRC32<Group>>;

    const size_t max_join_groups;
    const size_t max_grid_cells;
    std::mutex mutex;
    std::vector<PendingGroup> pending_groups;
    GroupIndexMap group_indices;
    size_t buffered_grid_cells = 0;
};

using PromQLTwoRangeRatesGroupStatePtr = std::shared_ptr<PromQLTwoRangeRatesGroupState>;

/// Immutable execution contract used when the exact two-rate island is fused
/// into an ordered MergeTree read. This is deliberately typed: a source cannot
/// install arbitrary pipeline callbacks while changing its declared header.
struct PromQLTwoRangeRatesFusionConfig
{
    using CollectorPtr = std::shared_ptr<ContextTimeSeriesTagsCollector>;

    PromQLTwoRangeRatesFusionConfig(
        CollectorPtr collector_,
        AggregateFunctionPtr rate_function_,
        String first_metric_name_,
        String second_metric_name_,
        size_t max_samples_per_series_,
        size_t max_output_block_size_,
        size_t max_join_groups_,
        size_t max_grid_cells_,
        std::optional<Field> raw_min_time_,
        std::optional<Field> raw_max_time_,
        SharedHeader output_header_);

    const CollectorPtr collector;
    const AggregateFunctionPtr rate_function;
    const String first_metric_name;
    const String second_metric_name;
    const size_t max_samples_per_series;
    const size_t max_output_block_size;
    const size_t max_join_groups;
    const size_t max_grid_cells;
    const std::optional<Field> raw_min_time;
    const std::optional<Field> raw_max_time;
    const SharedHeader output_header;
};

using PromQLTwoRangeRatesFusionConfigPtr = std::shared_ptr<const PromQLTwoRangeRatesFusionConfig>;

/// Shared implementation of metric-side matching and final grid addition.
/// Both the ordinary single-input transform and the storage-fused ordered
/// merge use this object, so they cannot drift in label or NULL semantics.
class PromQLTwoRangeRatesSeriesMatcher
{
public:
    using Collector = ContextTimeSeriesTagsCollector;
    using CollectorPtr = std::shared_ptr<Collector>;
    using Group = Collector::Group;

    PromQLTwoRangeRatesSeriesMatcher(
        CollectorPtr collector_,
        String first_metric_name_,
        String second_metric_name_,
        PromQLTwoRangeRatesGroupStatePtr group_state_);

    void addFinishedSeries(
        Group full_group,
        MutableColumnPtr & rate_result,
        MutableColumnPtr & group_column,
        MutableColumnPtr & values_column) const;

private:
    static void appendAddedGrid(
        const IColumn & first_column,
        size_t first_row,
        const IColumn & second_column,
        size_t second_row,
        MutableColumnPtr & output_column);

    const CollectorPtr collector;
    const String first_metric_name;
    const String second_metric_name;
    const PromQLTwoRangeRatesGroupStatePtr group_state;
};

using PromQLTwoRangeRatesSeriesMatcherPtr = std::shared_ptr<const PromQLTwoRangeRatesSeriesMatcher>;

/// A bounded single-stream kernel for
/// `rate(metric_a[window]) + rate(metric_b[window])`.
///
/// The input must be ordered by `(id, bucket)`. Each physical series is
/// reduced to one rate grid. The two metric names are joined on the full label
/// set after removing `__name__`, following PromQL's one-to-one matching rule.
/// Only matched groups are emitted; an unmatched side is discarded when input
/// is exhausted.
class PromQLTwoRangeRatesTransform final : public IProcessor
{
public:
    using Collector = ContextTimeSeriesTagsCollector;
    using CollectorPtr = std::shared_ptr<Collector>;
    using Group = Collector::Group;

    PromQLTwoRangeRatesTransform(
        SharedHeader input_header,
        CollectorPtr collector_,
        AggregateFunctionPtr rate_function_,
        String first_metric_name_,
        String second_metric_name_,
        size_t max_samples_per_series_,
        size_t max_output_block_size_,
        size_t max_join_groups_,
        size_t max_grid_cells_,
        PromQLTwoRangeRatesGroupStatePtr group_state_ = nullptr,
        std::optional<Field> raw_min_time_ = {},
        std::optional<Field> raw_max_time_ = {});

    ~PromQLTwoRangeRatesTransform() override;

    String getName() const override { return "PromQLTwoRangeRates"; }

    static SharedHeader transformHeader(const AggregateFunctionPtr & rate_function);

protected:
    Status prepare() override;
    void work() override;

private:
    void startSeries(const IColumn & id_column, size_t row, Group full_group);
    void finishSeries(MutableColumnPtr & group_column, MutableColumnPtr & values_column);
    void checkAndRememberInputOrder(const IColumn & id_column, const IColumn & bucket_column, size_t row);
    void destroyRateState() noexcept;

    InputPort & input;
    OutputPort & output;

    CollectorPtr collector;
    AggregateFunctionPtr rate_function;
    const size_t max_samples_per_series;
    const size_t max_output_block_size;
    PromQLTwoRangeRatesSeriesMatcherPtr series_matcher;

    size_t id_position = 0;
    size_t bucket_position = 0;
    size_t samples_position = 0;
    bool reads_raw_samples = false;

    AlignedBuffer rate_place;
    MutableColumnPtr current_id;
    MutableColumnPtr last_input_id;
    MutableColumnPtr last_input_bucket;
    MutableColumnPtr raw_min_time_column;
    MutableColumnPtr raw_max_time_column;
    MutableColumnPtr rate_result;
    PaddedPODArray<Group> full_groups;

    Chunk current_input_chunk;
    Chunk current_output_chunk;
    size_t current_input_row = 0;
    size_t current_series_samples = 0;
    Group current_full_group = Collector::getGroupForNoTags();
    bool has_input = false;
    bool has_current_series = false;
    bool rate_state_created = false;
    bool full_groups_ready = false;
    bool finishing_input = false;
};

}
