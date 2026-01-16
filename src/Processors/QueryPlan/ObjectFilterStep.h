#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

/// Implements WHERE condition only to filter objects in object storage
/// Difference with FilterStep is that ObjectFilterStep is added only for distributed calls
/// (table functions like `s3Cluster`) and is used only to filter objects,
/// not to filter data after reading, because initiator can have not this column
/// In query like `SELECT count() FROM s3Cluster('cluster', ...) WHERE key=42`
/// column `key` does not exists in blocks getting from cluster replicas.
class ObjectFilterStep : public IQueryPlanStep
{
public:
    ObjectFilterStep(
        const SharedHeader & input_header_,
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
