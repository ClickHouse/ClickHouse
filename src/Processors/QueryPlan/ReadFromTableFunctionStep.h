#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

class ReadFromTableFunctionStep : public ISourceStep
{
public:
    ReadFromTableFunctionStep(Block header, std::string serialized_ast_, TableExpressionModifiers table_expression_modifiers_);

    String getName() const override { return "ReadFromTableFunction"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    const std::string & getSerializedAST() const { return serialized_ast; }
    TableExpressionModifiers getTableExpressionModifiers() const { return table_expression_modifiers; }

private:
    std::string serialized_ast;
    TableExpressionModifiers table_expression_modifiers;
};

}
