#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

/// Create NullSource with specified structure.
class ReadFromTableStep : public ISourceStep
{
public:
    ReadFromTableStep(Block output_header, String table_name_, TableExpressionModifiers table_expression_modifiers_);

    String getName() const override { return "ReadFromTable"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(WriteBuffer & out) const override;
    static std::unique_ptr<IQueryPlanStep> deserialize(ReadBuffer & in, const DataStreams &, const DataStream * output, QueryPlanSerializationSettings &);

private:
    String table_name;
    TableExpressionModifiers table_expression_modifiers;
};

}
