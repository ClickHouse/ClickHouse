#pragma once

#include <memory>
#include <vector>

#include <Analyzer/HashUtils.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

struct SelectQueryOptions;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;
using QueryTreeNodes = std::vector<QueryTreeNodePtr>;

class FunctionNode;

class QueryPlan;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

class GlobalPlannerContext;
using GlobalPlannerContextPtr = std::shared_ptr<GlobalPlannerContext>;

/// `x IN (subquery)` where the subquery reads nothing from the outer query.
struct UncorrelatedInSubquery
{
    UncorrelatedInSubquery(QueryTreeNodes key_elements_, QueryTreeNodePtr subquery_, const String & action_node_name_, DataTypePtr result_type_, bool is_negated_)
    : key_elements(std::move(key_elements_))
    , subquery(std::move(subquery_))
    , action_node_name(action_node_name_)
    , result_type(std::move(result_type_))
    , is_negated(is_negated_)
    {}

    QueryTreeNodes key_elements;
    QueryTreeNodePtr subquery;
    String action_node_name;
    DataTypePtr result_type;
    bool is_negated = false;
    /// The columns the join keys on, one per element of the key. Filled by`analyzeInToJoin`.
    Names key_column_names;
    /// The same columns before the cast to the set type.
    Names key_column_names_before_cast;
};

using UncorrelatedInSubqueries = std::vector<UncorrelatedInSubquery>;

enum class InToJoinScope : uint8_t
{
    Where,
    Aggregation,
    Having,
    Window,
    Qualify,
    Projection,
    OrderBy,
    LimitBy,
};

/// The expressions the join keys on: the left argument of the `IN`, or its elements.
QueryTreeNodes getInToJoinKeyElements(const FunctionNode & function_node);

bool canRewriteInToJoin(
    const QueryTreeNodePtr & in_node,
    const QueryTreeNodePtr & query_node,
    const PlannerContext & planner_context);

void buildQueryPlanForUncorrelatedInSubquery(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    const UncorrelatedInSubquery & in_subquery,
    const SelectQueryOptions & select_query_options,
    GlobalPlannerContextPtr subquery_global_planner_context);

}
