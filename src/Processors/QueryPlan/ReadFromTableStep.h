#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Analyzer/TableExpressionModifiers.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

struct FilterDAGInfo;
using FilterDAGInfoPtr = std::shared_ptr<FilterDAGInfo>;

class ReadFromTableStep : public ISourceStep
{
public:
    ReadFromTableStep(
        SharedHeader header,
        String table_name_,
        TableExpressionModifiers table_expression_modifiers_,
        bool is_merge_tree_,
        FilterDAGInfoPtr row_policy_filter_,
        bool use_parallel_replicas_ = false);

    String getName() const override { return "ReadFromTable"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    const String & getTable() const { return table_name; }
    TableExpressionModifiers getTableExpressionModifiers() const { return table_expression_modifiers; }
    bool useParallelReplicas() const { return use_parallel_replicas; }
    bool & useParallelReplicas() { return use_parallel_replicas; }
    bool isMergeTree() const { return is_merge_tree; }
    FilterDAGInfoPtr getRowPolicyFilter() const { return row_policy_filter; }

    QueryPlanStepPtr clone() const override;

private:
    String table_name;
    TableExpressionModifiers table_expression_modifiers;
    bool is_merge_tree = false;
    FilterDAGInfoPtr row_policy_filter;
    bool use_parallel_replicas = false;
};

}
