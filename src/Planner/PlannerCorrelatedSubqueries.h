#pragma once

#include <memory>
#include <string_view>
#include <vector>

namespace DB
{

struct SelectQueryOptions;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;
using QueryTreeNodes = std::vector<QueryTreeNodePtr>;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

enum class CorrelatedSubqueryKind
{
    SCALAR,
    EXISTS,
};

struct CorrelatedSubquery
{
    CorrelatedSubquery(QueryTreeNodePtr query_tree_, CorrelatedSubqueryKind kind_)
        : query_tree(std::move(query_tree_))
        , kind(kind_)
    {}

    QueryTreeNodePtr query_tree;
    CorrelatedSubqueryKind kind;
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
    const CorrelatedSubquery & correlated_subquery,
    const SelectQueryOptions & select_query_options);

}
