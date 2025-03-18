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

struct CorrelatedSubtrees
{
    bool notEmpty() const noexcept { return !subqueries.empty(); }

    void assertEmpty(std::string_view reason) const;

    QueryTreeNodes subqueries;
};

void buildQueryPlanForCorrelatedSubquery(
    const QueryTreeNodePtr & correlated_subquery,
    const SelectQueryOptions & select_query_options);

}
