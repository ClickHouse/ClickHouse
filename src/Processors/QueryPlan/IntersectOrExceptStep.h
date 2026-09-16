#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>


namespace DB
{

class IntersectOrExceptStep : public IQueryPlanStep
{
public:
    using Operator = ASTSelectIntersectExceptQuery::Operator;

    /// max_threads is used to limit the number of threads for result pipeline.
    IntersectOrExceptStep(SharedHeaders input_headers_, Operator operator_, size_t max_threads_ = 0);

    String getName() const override { return "IntersectOrExcept"; }

    Operator getOperator() const { return current_operator; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    void describePipeline(FormatSettings & settings) const override;

    QueryPlanStepPtr clone() const override;

    /// Both inputs are hash-scattered by the whole row, so the output streams are disjoint by all columns.
    /// Zero means the step was deserialized on a worker and takes the executing server's `max_threads`, which
    /// is at least one; a single stream, whether from that or from the clamp on an extreme stream count, is
    /// trivially disjoint, so the property holds either way.
    bool isPartitioned() const { return max_threads != 1; }
    bool isSerializable() const override { return true; }
    void serialize(Serialization & ctx) const override;
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override;

    Operator current_operator;
    size_t max_threads;
};

}
