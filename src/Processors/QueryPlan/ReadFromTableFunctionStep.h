#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

class ReadFromTableFunctionStep : public ISourceStep
{
public:
    ReadFromTableFunctionStep(
        SharedHeader header,
        std::string serialized_ast_,
        TableExpressionModifiers table_expression_modifiers_,
        bool use_parallel_replicas_ = false);

    String getName() const override { return "ReadFromTableFunction"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    const std::string & getSerializedAST() const { return serialized_ast; }
    TableExpressionModifiers getTableExpressionModifiers() const { return table_expression_modifiers; }

    bool useParallelReplicas() const { return use_parallel_replicas; }
    bool & useParallelReplicas() { return use_parallel_replicas; }

private:
    std::string serialized_ast;
    TableExpressionModifiers table_expression_modifiers;
    bool use_parallel_replicas = false;
};

}
