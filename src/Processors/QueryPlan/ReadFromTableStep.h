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
        PrewhereInfoPtr prewhere_info_ = nullptr,
        FilterDAGInfoPtr row_level_filter_ = nullptr);

    String getName() const override { return "ReadFromTable"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    const String & getTable() const { return table_name; }
    TableExpressionModifiers getTableExpressionModifiers() const { return table_expression_modifiers; }
    bool useParallelReplicas() const { return use_parallel_replicas; }
    bool & useParallelReplicas() { return use_parallel_replicas; }
    PrewhereInfoPtr getPrewhereInfo() const { return prewhere_info; }
    FilterDAGInfoPtr getRowLevelFilter() const { return row_level_filter; }

    QueryPlanStepPtr clone() const override;
private:
    String table_name;
    TableExpressionModifiers table_expression_modifiers;
    bool use_parallel_replicas = false;
    PrewhereInfoPtr prewhere_info;
    /// Row-level security filter (row policy). Carried so that a cache hit re-applies the
    /// policy: `ReadFromMergeTree` keeps it in `SelectQueryInfo::row_level_filter`, and
    /// `resolveStorages` restores it onto the freshly bound `SelectQueryInfo`.
    FilterDAGInfoPtr row_level_filter;
};

}
