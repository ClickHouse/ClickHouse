#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/IColumn_fwd.h>
#include <Core/Field.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <Processors/IProcessor.h>
#include <Common/AlignedBuffer.h>
#include <Common/HashTable/HashSet.h>
#include <Common/PODArray.h>

#include <memory>
#include <mutex>
#include <optional>


namespace DB
{

class ColumnArray;

namespace PromQLRangeRateHelpers
{

/// Prometheus range selectors discard this exact NaN payload, but preserve other NaNs.
bool isStaleMarker(const IColumn & values, size_t sample);
bool containsStaleMarker(const IColumn & values, size_t begin, size_t end);
MutableColumnPtr filterStaleMarkers(const ColumnArray & samples, size_t row);

template <typename Callback>
void forEachNonStaleRange(const IColumn & values, size_t begin, size_t end, Callback && callback)
{
    size_t range_begin = begin;
    for (size_t sample = begin; sample < end; ++sample)
    {
        if (!isStaleMarker(values, sample))
            continue;

        if (range_begin < sample)
            callback(range_begin, sample);
        range_begin = sample + 1;
    }

    if (range_begin < end)
        callback(range_begin, end);
}

}

/// Query-wide uniqueness check for the post-`rate` label groups emitted by
/// parallel primary-key lanes.
class PromQLRangeRateGroupSet
{
public:
    bool tryRegister(UInt64 group)
    {
        std::lock_guard lock(mutex);
        return groups.insert(group).second;
    }

private:
    std::mutex mutex;
    HashSet<UInt64, HashCRC32<UInt64>> groups;
};

using PromQLRangeRateGroupSetPtr = std::shared_ptr<PromQLRangeRateGroupSet>;

/// A bounded native execution kernel for the PromQL shape
/// `rate(selector[window])`.
///
/// Input must be ordered by `(id, bucket)`. Consecutive rows for one physical
/// series are accumulated into one exact `timeSeriesRateToGrid` state. A
/// completed series is emitted as one `(group, values)` row after removing the
/// metric name from its full label group. Output chunks are capped without
/// splitting a physical series or buffering all output series.
class PromQLRangeRateTransform final : public IProcessor
{
public:
    using Collector = ContextTimeSeriesTagsCollector;
    using CollectorPtr = std::shared_ptr<Collector>;
    using Group = Collector::Group;

    PromQLRangeRateTransform(
        SharedHeader input_header,
        CollectorPtr collector_,
        AggregateFunctionPtr rate_function_,
        size_t max_samples_per_series_,
        size_t max_output_block_size_,
        PromQLRangeRateGroupSetPtr output_groups_ = nullptr,
        std::optional<Field> raw_min_time_ = {},
        std::optional<Field> raw_max_time_ = {});

    ~PromQLRangeRateTransform() override;

    String getName() const override { return "PromQLRangeRate"; }

    static SharedHeader transformHeader(const AggregateFunctionPtr & rate_function);

protected:
    Status prepare() override;
    void work() override;

private:
    void startSeries(const IColumn & id_column, const IColumn & bucket_column, size_t row, Group full_group);
    void finishSeries(MutableColumnPtr & group_column, MutableColumnPtr & values_column);
    void checkAndRememberInputOrder(const IColumn & id_column, const IColumn & bucket_column, size_t row);
    void updateCurrentBucket(const IColumn & bucket_column, size_t row);
    void destroyRateState() noexcept;

    InputPort & input;
    OutputPort & output;

    CollectorPtr collector;
    AggregateFunctionPtr rate_function;
    const size_t max_samples_per_series;
    const size_t max_output_block_size;
    PromQLRangeRateGroupSetPtr output_groups;

    size_t id_position = 0;
    size_t bucket_position = 0;
    size_t samples_position = 0;
    bool reads_raw_samples = false;

    AlignedBuffer rate_place;
    MutableColumnPtr current_id;
    MutableColumnPtr current_bucket;
    MutableColumnPtr last_input_id;
    MutableColumnPtr last_input_bucket;
    MutableColumnPtr raw_min_time_column;
    MutableColumnPtr raw_max_time_column;
    PaddedPODArray<Group> full_groups;

    Chunk current_input_chunk;
    Chunk current_output_chunk;
    size_t current_input_row = 0;
    size_t current_series_samples = 0;
    Group current_output_group = Collector::getGroupForNoTags();
    bool has_input = false;
    bool has_current_series = false;
    bool rate_state_created = false;
    bool full_groups_ready = false;
    bool finishing_input = false;
};

}
