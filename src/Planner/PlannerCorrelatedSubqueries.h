#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

struct SelectQueryOptions;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;
using QueryTreeNodes = std::vector<QueryTreeNodePtr>;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

using ColumnIdentifier = std::string;
using ColumnIdentifiers = std::vector<ColumnIdentifier>;

enum class CorrelatedSubqueryKind
{
    SCALAR,
    EXISTS,
};

struct CorrelatedSubquery
{
    CorrelatedSubquery(QueryTreeNodePtr query_tree_, CorrelatedSubqueryKind kind_, const String & action_node_name_, ColumnIdentifiers correlated_column_identifiers_)
        : query_tree(std::move(query_tree_))
        , kind(kind_)
        , action_node_name(action_node_name_)
        , correlated_column_identifiers(std::move(correlated_column_identifiers_))
    {}

    QueryTreeNodePtr query_tree;
    CorrelatedSubqueryKind kind;
    String action_node_name;
    ColumnIdentifiers correlated_column_identifiers;
};

using CorrelatedSubqueries = std::vector<CorrelatedSubquery>;

struct CorrelatedSubtrees
{
    bool notEmpty() const noexcept { return !subqueries.empty(); }

    void assertEmpty(std::string_view reason) const;

    CorrelatedSubqueries subqueries;
};

void buildQueryPlanForCorrelatedSubquery(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    const CorrelatedSubquery & correlated_subquery,
    const SelectQueryOptions & select_query_options);

}
