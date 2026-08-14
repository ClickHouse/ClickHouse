#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/RuntimeFilterGeometry.h>

namespace DB
{

/// Passes the probe-side stream through unchanged while collecting this join's runtime filter
/// state over a dedicated exchange: the complete union merged by the filter's merge tree (or the
/// single build task's partial when the build stage has one task). Once every expected state has
/// arrived, the union is registered in this task's filter map, and the `__applyFilter` below
/// starts pruning; until then (or if a state never arrives) rows pass unfiltered.
class ReceiveRuntimeFilterStep final : public IQueryPlanStep
{
public:
    ReceiveRuntimeFilterStep(
        const SharedHeader & input_header_,
        String filter_name_,
        String filter_key_,
        const DataTypePtr & filter_column_type_,
        const RuntimeFilterGeometry & geometry_);

    ReceiveRuntimeFilterStep(const ReceiveRuntimeFilterStep & other) = default;

    String getName() const override { return "ReceiveRuntimeFilter"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    const String & getFilterName() const { return filter_name; }
    const String & getFilterKey() const { return filter_key; }

    /// Called when the distributed plan is split into stages and the filter exchange becomes known.
    /// One partial is expected from each of the source buckets (the build-side task buckets).
    void setExchange(const String & exchange_id_, Strings source_buckets_);

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;

    String filter_name;
    String filter_key;
    DataTypePtr filter_column_type;

    RuntimeFilterGeometry geometry;

    /// Empty until the distributed split assigns the filter exchange; the step is then a passthrough.
    String exchange_id;
    Strings source_buckets;
};

}
