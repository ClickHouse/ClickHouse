#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

struct ObjectFilterWire;

/// Implements WHERE condition only to filter objects in object storage
/// Difference with FilterStep is that ObjectFilterStep is added only for distributed calls
/// (table functions like `s3Cluster`) and is used only to filter objects,
/// not to filter data after reading, because initiator can have not this column
/// In query like `SELECT count() FROM s3Cluster('cluster', ...) WHERE key=42`
/// column `key` does not exist in blocks getting from cluster replicas.
class ObjectFilterStep : public IQueryPlanStep
{
public:
    ObjectFilterStep(
        const SharedHeader & input_header_,
        ActionsDAG actions_dag_,
        String filter_column_name_);

    String getName() const override { return "ObjectFilter"; }
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    bool hasCorrelatedExpressions() const override { return actions_dag.hasCorrelatedColumns(); }

    const ActionsDAG & getExpression() const { return actions_dag; }
    ActionsDAG & getExpression() { return actions_dag; }
    const String & getFilterColumnName() const { return filter_column_name; }

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `ObjectFilterStep.cpp` declares.
    ObjectFilterWire toWire() const;
    static QueryPlanStepPtr fromWire(ObjectFilterWire wire, Deserialization & ctx);

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    void updateOutputHeader() override;

    ActionsDAG actions_dag;
    String filter_column_name;
};

/// What `ObjectFilterStep` puts on the wire in the framed format.
struct ObjectFilterWire
{
    ActionsDAG actions_dag;
    String filter_column_name;
};

}
