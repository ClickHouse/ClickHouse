#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

class ReadFromTableStep : public ISourceStep
{
public:
    /// Whether the plan applies the table's SELECT row policy: `FilterStep` - a filter step above this read
    /// does; `NotInPlan` - no step does (the sender pushed the policy into the read, which is not serialized,
    /// or had none); `Unknown` - the sender did not record it.
    enum class RowPolicyPlacement : uint8_t
    {
        Unknown,
        NotInPlan,
        FilterStep,
    };

    ReadFromTableStep(
        SharedHeader header,
        String table_name_,
        TableExpressionModifiers table_expression_modifiers_,
        bool use_parallel_replicas_ = false,
        RowPolicyPlacement row_policy_placement_ = RowPolicyPlacement::Unknown);

    String getName() const override { return "ReadFromTable"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    const String & getTable() const { return table_name; }
    TableExpressionModifiers getTableExpressionModifiers() const { return table_expression_modifiers; }
    bool useParallelReplicas() const { return use_parallel_replicas; }
    bool & useParallelReplicas() { return use_parallel_replicas; }
    RowPolicyPlacement getRowPolicyPlacement() const { return row_policy_placement; }

    QueryPlanStepPtr clone() const override;
private:
    String table_name;
    TableExpressionModifiers table_expression_modifiers;
    bool use_parallel_replicas = false;
    RowPolicyPlacement row_policy_placement = RowPolicyPlacement::Unknown;
};

}
