#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

class ReadFromTableFunctionStep : public ISourceStep
{
public:
    ReadFromTableFunctionStep(SharedHeader header, std::string serialized_ast_, TableExpressionModifiers table_expression_modifiers_);

    [[nodiscard]] String getName() const override { return "ReadFromTableFunction"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    [[nodiscard]] bool isSerializable() const override { return true; }
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    [[nodiscard]] const std::string & getSerializedAST() const { return serialized_ast; }
    [[nodiscard]] TableExpressionModifiers getTableExpressionModifiers() const { return table_expression_modifiers; }

private:
    std::string serialized_ast;
    TableExpressionModifiers table_expression_modifiers;
};

}
