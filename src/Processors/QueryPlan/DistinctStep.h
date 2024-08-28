#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

/// Execute DISTINCT for specified columns.
class DistinctStep : public ITransformingStep
{
public:
    DistinctStep(
        const DataStream & input_stream_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        bool pre_distinct_, /// If is enabled, execute distinct for separate streams. Otherwise, merge streams.
        bool optimize_distinct_in_order_);

    String getName() const override { return "Distinct"; }
    const Names & getColumnNames() const { return columns; }

    String getSerializationName() const override { return pre_distinct ? "PreDistinct" : "Distinct"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    bool isPreliminary() const { return pre_distinct; }

    UInt64 getLimitHint() const { return limit_hint; }
    void updateLimitHint(UInt64 hint);

    void serializeSettings(QueryPlanSerializationSettings & settings) const override;
    void serialize(WriteBuffer & out) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(ReadBuffer & in, const DataStreams & input_streams_, QueryPlanSerializationSettings &, bool pre_distinct_);
    static std::unique_ptr<IQueryPlanStep> deserializeNormal(ReadBuffer & in, const DataStreams & input_streams_, QueryPlanSerializationSettings &);
    static std::unique_ptr<IQueryPlanStep> deserializePre(ReadBuffer & in, const DataStreams & input_streams_, QueryPlanSerializationSettings &);

private:
    void updateOutputStream() override;

    SizeLimits set_size_limits;
    UInt64 limit_hint;
    const Names columns;
    bool pre_distinct;
    bool optimize_distinct_in_order;
};

}
