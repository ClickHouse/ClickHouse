#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/IColumn_fwd.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/Transforms/PromQLGroupLimit.h>
#include <Common/AlignedBuffer.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/PODArray.h>

#include <memory>
#include <vector>


namespace DB
{

/// A bounded native execution kernel for the PromQL shape
/// `sum by (...) (rate(selector[window]))`.
///
/// Input must be ordered by `(id, bucket)`. Consecutive rows for one physical
/// series are accumulated into one exact `timeSeriesRateToGrid` state. When the
/// identifier changes, its grid is immediately folded into a `sumForEach`
/// state for the projected output group, so rate state is bounded to one
/// physical series while the output state is bounded to one grid per group.
class PromQLRangeSumByTransform final : public IAccumulatingTransform
{
public:
    using Collector = ContextTimeSeriesTagsCollector;
    using CollectorPtr = std::shared_ptr<Collector>;
    using Group = Collector::Group;

    PromQLRangeSumByTransform(
        SharedHeader input_header,
        CollectorPtr collector_,
        AggregateFunctionPtr rate_function_,
        AggregateFunctionPtr sum_function_,
        Strings labels_to_keep_,
        size_t max_samples_per_series_,
        size_t max_output_groups_,
        size_t max_output_block_size_,
        /// Optional query-wide output-group limit shared by parallel lanes and their merge.
        PromQLGroupLimitPtr group_limit_ = nullptr);

    ~PromQLRangeSumByTransform() override;

    String getName() const override { return "PromQLRangeSumBy"; }

    static SharedHeader transformHeader(const AggregateFunctionPtr & sum_function);

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    void startSeries(const IColumn & id_column, size_t row, Group full_group);
    void finishSeries();
    Group projectGroup(Group full_group);
    AggregateDataPtr getOrCreateGroupState(Group group);
    void destroyStates() noexcept;

    CollectorPtr collector;
    AggregateFunctionPtr rate_function;
    AggregateFunctionPtr sum_function;
    Strings labels_to_keep;
    const size_t max_samples_per_series;
    const size_t max_output_groups;
    const size_t max_output_block_size;
    const PromQLGroupLimitPtr group_limit;

    size_t id_position = 0;
    size_t time_series_position = 0;

    AlignedBuffer rate_place;
    MutableColumnPtr current_id;
    MutableColumnPtr rate_result;
    std::unique_ptr<Arena> group_arena;
    HashMap<Group, AggregateDataPtr, HashCRC32<Group>> group_states;
    PaddedPODArray<Group> full_groups;
    std::vector<Group> generated_groups;
    size_t next_generated_group = 0;

    Group current_output_group = Collector::getGroupForNoTags();
    size_t current_series_samples = 0;
    bool rate_state_created = false;
    bool has_current_series = false;
    bool generation_started = false;
};

}
