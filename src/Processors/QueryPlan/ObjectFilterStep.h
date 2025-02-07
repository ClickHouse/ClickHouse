#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

/// Implements WHERE operation.
class ObjectFilterStep : public IQueryPlanStep
{
public:
    ObjectFilterStep(
        const Header & input_header_,
        ActionsDAG actions_dag_,
        String filter_column_name_);

    String getName() const override { return "ObjectFilter"; }
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    const ActionsDAG & getExpression() const { return actions_dag; }
    ActionsDAG & getExpression() { return actions_dag; }
    const String & getFilterColumnName() const { return filter_column_name; }

    void serialize(Serialization & ctx) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override;

    ActionsDAG actions_dag;
    String filter_column_name;
};

}
