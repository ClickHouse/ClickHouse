#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>


namespace DB
{

struct IntersectOrExceptWire;

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

    bool isSerializable() const override { return true; }
    void serialize(Serialization & ctx) const override;
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `IntersectOrExceptStep.cpp` declares.
    IntersectOrExceptWire toWire() const;
    static QueryPlanStepPtr fromWire(IntersectOrExceptWire wire, Deserialization & ctx);

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);

    void updateOutputHeader() override;

    Operator current_operator;
    size_t max_threads;
};

}
