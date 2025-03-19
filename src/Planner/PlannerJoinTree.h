#pragma once

#include <Storages/SelectQueryInfo.h>

#include <Interpreters/SelectQueryOptions.h>

#include <Processors/QueryPlan/QueryPlan.h>

#include <Planner/PlannerContext.h>

namespace DB
{

using UsefulSets = std::unordered_set<FutureSetPtr>;

struct JoinTreeQueryPlan
{
    QueryPlan query_plan;
    QueryProcessingStage::Enum from_stage;
    std::set<std::string> used_row_policies{};
    UsefulSets useful_sets{};
    std::unordered_map<const QueryNode *, const QueryPlan::Node *> query_node_to_plan_step_mapping{};

    /// When root node is JoinStepLogical this map contains mapping from table expression sources to step input numbers
    /// For example, if we have a join like `SELECT * FROM t1 JOIN t2 USING (a) JOIN t3 USING (b)`,
    /// then this map will contain mapping from t1, t2, t3 to 0, 1, 2 respectively
    std::unordered_map<const IQueryTreeNode *, size_t> table_expression_to_join_input_mapping{};

};

/// Build JOIN TREE query plan for query node
JoinTreeQueryPlan buildJoinTreeQueryPlan(const QueryTreeNodePtr & query_node,
    const SelectQueryInfo & select_query_info,
    SelectQueryOptions & select_query_options,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context);

}
