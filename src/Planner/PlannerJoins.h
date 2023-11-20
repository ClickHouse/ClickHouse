#pragma once

#include <Core/Joins.h>
#include <Core/ColumnsWithTypeAndName.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/IJoin.h>

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/** Join clause represent single JOIN ON section clause.
  * Join clause consists of JOIN keys and conditions.
  *
  * JOIN can contain multiple clauses in JOIN ON section.
  * Example: SELECT * FROM test_table_1 AS t1 INNER JOIN test_table_2 AS t2 ON t1.id = t2.id OR t1.value = t2.value;
  * t1.id = t2.id is first clause.
  * t1.value = t2.value is second clause.
  *
  * JOIN ON section can also contain condition inside clause.
  * Example: SELECT * FROM test_table_1 AS t1 INNER JOIN test_table_2 AS t2 ON t1.id = t2.id AND t1.id > 0 AND t2.id > 0;
  * t1.id = t2.id AND t1.id > 0 AND t2.id > 0 is first clause.
  * t1.id = t2.id is JOIN keys section.
  * t1.id > 0 is left table condition.
  * t2.id > 0 is right table condition.
  *
  * Additionally not only conditions, but JOIN keys can be represented as expressions.
  * Example: SELECT * FROM test_table_1 AS t1 INNER JOIN test_table_2 AS t2 ON toString(t1.id) = toString(t2.id).
  * toString(t1.id) = toString(t2.id) is JOIN keys section. Where toString(t1.id) is left key, and toString(t2.id) is right key.
  *
  * During query planning JOIN ON section represented using join clause structure. It is important to split
  * keys and conditions. And for each action detect from which stream it can be performed.
  *
  * We have 2 streams, left stream and right stream.
  * We split JOIN ON section expressions actions in two parts left join expression actions and right join expression actions.
  * Left join expression actions must be used to calculate necessary actions for left stream.
  * Right join expression actions must be used to calculate necessary actions for right stream.
  */
class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

struct ASOFCondition
{
    size_t key_index;
    ASOFJoinInequality asof_inequality;
};

/// Single JOIN ON section clause representation
class JoinClause
{
public:
    /// Add keys
    void addKey(const ActionsDAG::Node * left_key_node, const ActionsDAG::Node * right_key_node, bool null_safe_comparison = false)
    {
        left_key_nodes.emplace_back(left_key_node);
        right_key_nodes.emplace_back(right_key_node);
        if (null_safe_comparison)
            nullsafe_compare_key_indexes.emplace(left_key_nodes.size() - 1);
    }

    void addASOFKey(const ActionsDAG::Node * left_key_node, const ActionsDAG::Node * right_key_node, ASOFJoinInequality asof_inequality)
    {
        left_key_nodes.emplace_back(left_key_node);
        right_key_nodes.emplace_back(right_key_node);
        asof_conditions.push_back(ASOFCondition{left_key_nodes.size() - 1, asof_inequality});
    }

    /// Add condition for table side
    void addCondition(JoinTableSide table_side, const ActionsDAG::Node * condition_node)
    {
        auto & filter_condition_nodes = table_side == JoinTableSide::Left ? left_filter_condition_nodes : right_filter_condition_nodes;
        filter_condition_nodes.push_back(condition_node);
    }

    /// Get left key nodes
    const ActionsDAG::NodeRawConstPtrs & getLeftKeyNodes() const
    {
        return left_key_nodes;
    }

    /// Get left key nodes
    ActionsDAG::NodeRawConstPtrs & getLeftKeyNodes()
    {
        return left_key_nodes;
    }

    /// Get right key nodes
    const ActionsDAG::NodeRawConstPtrs & getRightKeyNodes() const
    {
        return right_key_nodes;
    }

    /// Get right key nodes
    ActionsDAG::NodeRawConstPtrs & getRightKeyNodes()
    {
        return right_key_nodes;
    }

    bool isNullsafeCompareKey(size_t idx) const
    {
        return nullsafe_compare_key_indexes.contains(idx);
    }

    /// Returns true if JOIN clause has ASOF conditions, false otherwise
    bool hasASOF() const
    {
        return !asof_conditions.empty();
    }

    /// Get ASOF conditions
    const std::vector<ASOFCondition> & getASOFConditions() const
    {
        return asof_conditions;
    }

    /// Get left filter condition nodes
    const ActionsDAG::NodeRawConstPtrs & getLeftFilterConditionNodes() const
    {
        return left_filter_condition_nodes;
    }

    /// Get left filter condition nodes
    ActionsDAG::NodeRawConstPtrs & getLeftFilterConditionNodes()
    {
        return left_filter_condition_nodes;
    }

    /// Get right filter condition nodes
    const ActionsDAG::NodeRawConstPtrs & getRightFilterConditionNodes() const
    {
        return right_filter_condition_nodes;
    }

    /// Get right filter condition nodes
    ActionsDAG::NodeRawConstPtrs & getRightFilterConditionNodes()
    {
        return right_filter_condition_nodes;
    }

    /// Dump clause into buffer
    void dump(WriteBuffer & buffer) const;

    /// Dump clause
    String dump() const;

private:
    ActionsDAG::NodeRawConstPtrs left_key_nodes;
    ActionsDAG::NodeRawConstPtrs right_key_nodes;

    std::vector<ASOFCondition> asof_conditions;

    ActionsDAG::NodeRawConstPtrs left_filter_condition_nodes;
    ActionsDAG::NodeRawConstPtrs right_filter_condition_nodes;

    std::unordered_set<size_t> nullsafe_compare_key_indexes;
};

using JoinClauses = std::vector<JoinClause>;

struct JoinClausesAndActions
{
    /// Join clauses. Actions dag nodes point into join_expression_actions.
    JoinClauses join_clauses;
    /// Whole JOIN ON section expressions
    ActionsDAGPtr join_expression_actions;
    /// Left join expressions actions
    ActionsDAGPtr left_join_expressions_actions;
    /// Right join expressions actions
    ActionsDAGPtr right_join_expressions_actions;
};

/** Calculate join clauses and actions for JOIN ON section.
  *
  * left_table_expression_columns - columns from left join stream.
  * right_table_expression_columns - columns from right join stream.
  * join_node - join query tree node.
  * planner_context - planner context.
  */
JoinClausesAndActions buildJoinClausesAndActions(
    const ColumnsWithTypeAndName & left_table_expression_columns,
    const ColumnsWithTypeAndName & right_table_expression_columns,
    const QueryTreeNodePtr & join_node,
    const PlannerContextPtr & planner_context);

/** Try extract boolean constant from JOIN expression.
  * Example: SELECT * FROM test_table AS t1 INNER JOIN test_table AS t2 ON 1;
  * Example: SELECT * FROM test_table AS t1 INNER JOIN test_table AS t2 ON 1 != 1;
  *
  * join_node - join query tree node.
  */
std::optional<bool> tryExtractConstantFromJoinNode(const QueryTreeNodePtr & join_node);

/** Choose JOIN algorithm for table join, right table expression, right table expression header and planner context.
  * Table join structure can be modified during JOIN algorithm choosing for special JOIN algorithms.
  * For example JOIN with Dictionary engine, or JOIN with JOIN engine.
  */
std::shared_ptr<IJoin> chooseJoinAlgorithm(std::shared_ptr<TableJoin> & table_join,
    const QueryTreeNodePtr & right_table_expression,
    const Block & left_table_expression_header,
    const Block & right_table_expression_header,
    const PlannerContextPtr & planner_context);

}
