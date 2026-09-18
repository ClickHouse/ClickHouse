#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/IColumn_fwd.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <Processors/IAccumulatingTransform.h>
#include <Common/AlignedBuffer.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/PODArray.h>

#include <memory>
#include <mutex>


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

    /// Query-owned guard shared by all ordered range-sum lanes. It rejects a
    /// full tag set as soon as a second physical series tries to register it.
    /// The check and insertion are one operation so two lanes cannot both
    /// admit the same full group.
    struct FullGroupGuard
    {
        std::mutex mutex;
        HashSet<Group, HashCRC32<Group>> groups;
    };

    using FullGroupGuardPtr = std::shared_ptr<FullGroupGuard>;

    enum class SeriesDictionaryReadiness
    {
        /// A dedicated tags pipeline published a complete native dictionary.
        PublishedNativeDictionary,

        /// The selector's `id IN (tags subquery)` dependency populated the ordinary collector.
        /// Missing identifiers still fail closed in `getGroupByID`.
        SelectorSetDependency,
    };

    PromQLRangeSumByTransform(
        SharedHeader input_header,
        CollectorPtr collector_,
        AggregateFunctionPtr rate_function_,
        AggregateFunctionPtr sum_function_,
        Strings labels_to_keep_,
        size_t max_output_groups_,
        SeriesDictionaryReadiness dictionary_readiness_ = SeriesDictionaryReadiness::PublishedNativeDictionary,
        /// Optional query-wide duplicate full-group check shared by parallel lanes.
        FullGroupGuardPtr full_group_guard_ = nullptr);

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
    const size_t max_output_groups;
    const SeriesDictionaryReadiness dictionary_readiness;
    const FullGroupGuardPtr full_group_guard;

    size_t id_position = 0;
    size_t time_series_position = 0;

    AlignedBuffer rate_place;
    MutableColumnPtr current_id;
    MutableColumnPtr rate_result;
    Arena group_arena;
    HashMap<Group, AggregateDataPtr, HashCRC32<Group>> group_states;
    HashSet<Group, HashCRC32<Group>> seen_full_groups;
    PaddedPODArray<Group> full_groups;

    Group current_output_group = Collector::getGroupForNoTags();
    bool rate_state_created = false;
    bool has_current_series = false;
    bool generated = false;
};

}
