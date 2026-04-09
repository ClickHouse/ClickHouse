#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Analyzer/TableExpressionModifiers.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class ReadFromTableStep : public ISourceStep
{
public:
    ReadFromTableStep(
        SharedHeader header,
        String table_name_,
        TableExpressionModifiers table_expression_modifiers_,
        bool use_parallel_replicas_ = false,
        PrewhereInfoPtr prewhere_info_ = nullptr);

    String getName() const override { return "ReadFromTable"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    const String & getTable() const { return table_name; }
    TableExpressionModifiers getTableExpressionModifiers() const { return table_expression_modifiers; }
    bool useParallelReplicas() const { return use_parallel_replicas; }
    bool & useParallelReplicas() { return use_parallel_replicas; }
    PrewhereInfoPtr getPrewhereInfo() const { return prewhere_info; }

    QueryPlanStepPtr clone() const override;
private:
    String table_name;
    TableExpressionModifiers table_expression_modifiers;
    bool use_parallel_replicas = false;
    PrewhereInfoPtr prewhere_info;
};

}
