#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/RuntimeFilterGeometry.h>

namespace DB
{

/// Fan-in of the runtime filter merge tree: a merge task receives at most this many child states.
/// A larger fan-in costs memory in each merge task: every streaming exchange input holds at most one
/// in-flight packet in userspace plus a bounded socket buffer, so a task's memory is O(fan_in),
/// independent of the total task count. A smaller fan-in adds merge tasks: S / fan_in at the first
/// level for S build tasks. With the bucket-count cap of 256, a fan-in of 16 keeps the tree at most
/// two levels deep. It is a constant, not a setting: it must not depend on cluster size, so that the
/// topology stays O(S + D) streams for D receiving tasks.
constexpr size_t RUNTIME_FILTER_MERGE_FAN_IN = 16;

/// The sole step of an intermediate stage of the runtime filter merge tree. It receives serialized
/// partial filter states from its child tasks - build tasks or lower merge tasks - over one
/// exchange, and merges them incrementally into a single state. That state goes to its parent, or,
/// at the tree root, to every task of every receiving stage. The fragment is complete on its own:
/// exchange sources feed a `MergeRuntimeFiltersTransform` whose output ends in exchange sinks.
///
/// The step itself is stage-invariant; which slice of child buckets a task consumes and which
/// parent it feeds are derived from the task's `bucket_id` parameter: task `i` consumes child
/// buckets `[i * fan_in, (i + 1) * fan_in)` of `source_buckets` and feeds parent `i / fan_in` at
/// the next level. The wiring in `RuntimeFilterExchangeWiring.cpp` enumerates the exchange
/// streams with the same rule, so the plan's stream lists and the built pipeline always agree.
class MergeRuntimeFiltersStep final : public IQueryPlanStep
{
public:
    struct Output
    {
        String exchange_id;
        /// Explicit destination buckets: the root broadcasting to one receiving stage's tasks.
        /// Empty: the destination is this task's parent merge task, `bucket index / fan_in`.
        Strings destination_buckets;
    };

    MergeRuntimeFiltersStep(
        String filter_name_,
        const DataTypePtr & filter_column_type_,
        const RuntimeFilterGeometry & geometry_,
        String input_exchange_id_,
        Strings source_buckets_,
        size_t fan_in_,
        std::vector<Output> outputs_);

    MergeRuntimeFiltersStep(const MergeRuntimeFiltersStep & other) = default;

    String getName() const override { return "MergeRuntimeFilters"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override { }

    String filter_name;
    DataTypePtr filter_column_type;
    RuntimeFilterGeometry geometry;

    String input_exchange_id;
    /// Ordered buckets of the level below; each task consumes its `fan_in`-sized slice.
    Strings source_buckets;
    size_t fan_in;
    std::vector<Output> outputs;
};

}
