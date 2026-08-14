#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/RuntimeFilterGeometry.h>

#include <optional>

namespace DB
{

/// Build-side counterpart of `ReceiveRuntimeFilterStep`: takes the place of `BuildRuntimeFilterStep`
/// when the filter has to cross task boundaries. Builds one partial per task from the stream it
/// passes through and ships it, serialized, over a dedicated exchange at end of stream.
class SendRuntimeFilterStep final : public IQueryPlanStep
{
public:
    SendRuntimeFilterStep(
        const SharedHeader & input_header_,
        String filter_column_name_,
        const DataTypePtr & filter_column_type_,
        String filter_name_,
        String filter_key_,
        const RuntimeFilterGeometry & geometry_);

    SendRuntimeFilterStep(const SendRuntimeFilterStep & other) = default;

    String getName() const override { return "SendRuntimeFilter"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    const String & getFilterColumnName() const { return filter_column_name; }
    const DataTypePtr & getFilterColumnType() const { return filter_column_type; }
    const String & getFilterName() const { return filter_name; }
    const String & getFilterKey() const { return filter_key; }
    const RuntimeFilterGeometry & getGeometry() const { return geometry; }

    /// See `ReceiveRuntimeFilterStep::setExchange`; the destinations are the probe-side task
    /// buckets. One filter may be applied in several probe-side stages, so the partials go out
    /// over one exchange per receiving stage. Used when the build stage has a single task (the
    /// task is then the root of the merge tree and broadcasts directly).
    void addExchange(const String & exchange_id_, Strings destination_buckets_);

    /// The build stage has several tasks and the partials go through a merge tree: each build task
    /// sends its partial once, to its parent merge task, computed from the task's position in
    /// `source_buckets` (the ordered buckets of the build stage) as `index / fan_in`. Mutually
    /// exclusive with `addExchange`.
    void setTreeExchange(const String & exchange_id_, Strings source_buckets_, size_t fan_in_);

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;

    String filter_column_name;
    DataTypePtr filter_column_type;
    String filter_name;
    /// Random per-plan-build rendezvous key, used only to pair this step with its
    /// `ReceiveRuntimeFilterStep` when the plan is split into stages. Not serialized: the build side
    /// registers nothing, so a worker never needs it.
    String filter_key;

    RuntimeFilterGeometry geometry;

    struct FilterExchange
    {
        String exchange_id;
        Strings destination_buckets;
    };

    struct TreeExchange
    {
        String exchange_id;
        /// Ordered buckets of the build stage; a task's parent is `own index / fan_in`.
        Strings source_buckets;
        size_t fan_in = 0;
    };

    /// Both empty (step is a passthrough) until the distributed split assigns the filter
    /// exchange(s); then exactly one of the two is set.
    std::vector<FilterExchange> exchanges;
    std::optional<TreeExchange> tree_exchange;
};

}
