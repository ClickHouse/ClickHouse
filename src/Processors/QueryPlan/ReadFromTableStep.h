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

    const String & getTable() const { return table_name; }
    TableExpressionModifiers getTableExpressionModifiers() const { return table_expression_modifiers; }

private:
    String table_name;
    TableExpressionModifiers table_expression_modifiers;
};

}
