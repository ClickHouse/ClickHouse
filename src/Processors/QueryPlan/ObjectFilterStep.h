#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

<<<<<<< HEAD
/// Implements WHERE condition only to filter objects in object storage
/// Difference with FilterStep is that ObjectFilterStep is added only for distributed calls
/// (table functions like `s3Cluster`) and is used only to filter objects,
/// not to filter data after reading, because initiator can have not this column
/// In query like `SELECT count() FROM s3Cluster('cluster', ...) WHERE key=42`
/// column `key` does not exist in blocks getting from cluster replicas.
=======
/// Implements WHERE operation.
>>>>>>> d9d3710bd9b (Merge pull request #1646 from Altinity/frontport/antalya-26.3/fix_remote_calls)
class ObjectFilterStep : public IQueryPlanStep
{
public:
    ObjectFilterStep(
<<<<<<< HEAD
        const SharedHeader & input_header_,
=======
        SharedHeader input_header_,
>>>>>>> d9d3710bd9b (Merge pull request #1646 from Altinity/frontport/antalya-26.3/fix_remote_calls)
        ActionsDAG actions_dag_,
        String filter_column_name_);

    String getName() const override { return "ObjectFilter"; }
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

<<<<<<< HEAD
    bool hasCorrelatedExpressions() const override { return actions_dag.hasCorrelatedColumns(); }

=======
>>>>>>> d9d3710bd9b (Merge pull request #1646 from Altinity/frontport/antalya-26.3/fix_remote_calls)
    const ActionsDAG & getExpression() const { return actions_dag; }
    ActionsDAG & getExpression() { return actions_dag; }
    const String & getFilterColumnName() const { return filter_column_name; }

    void serialize(Serialization & ctx) const override;
<<<<<<< HEAD
    bool isSerializable() const override { return true; }
=======
>>>>>>> d9d3710bd9b (Merge pull request #1646 from Altinity/frontport/antalya-26.3/fix_remote_calls)

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override;

    ActionsDAG actions_dag;
    String filter_column_name;
};

}
