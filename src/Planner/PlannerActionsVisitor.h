#pragma once

#include <optional>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>

#include <Interpreters/Context_fwd.h>

#include <Analyzer/IQueryTreeNode.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/WindowDescription.h>

namespace DB
{

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

/** Planner actions visitor is responsible for adding necessary actions to calculate query tree expression node
  * into actions dag.
  *
  * Preconditions:
  * 1. Table expression data for table expression nodes is collected in planner context.
  * For column node, that has column table expression source, identifier for column name in table expression data
  * is used as action dag node name, if use_column_identifier_as_action_node_name = true.
  * 2. Sets for IN functions are already collected in planner context.
  *
  * During actions build, there is special handling for following functions:
  * 1. Aggregate functions are added in actions dag as INPUT nodes. Aggregate functions arguments are not added.
  * 2. For function `in` and its variants, already collected sets from planner context are used.
  */
class PlannerActionsVisitor
{
public:
    explicit PlannerActionsVisitor(const PlannerContextPtr & planner_context_, bool use_column_identifier_as_action_node_name_ = true);

    /** Add actions necessary to calculate expression node into expression dag.
      * Necessary actions are not added in actions dag output.
      * Returns query tree expression node actions dag nodes.
      */
    ActionsDAG::NodeRawConstPtrs visit(ActionsDAG & actions_dag, QueryTreeNodePtr expression_node);

private:
    const PlannerContextPtr planner_context;
    bool use_column_identifier_as_action_node_name = true;
};

/** Calculate query tree expression node action dag name and add them into node to name map.
  * If node exists in map, name from map is used.
  *
  * For column node column node identifier from planner context is used, if use_column_identifier_as_action_node_name = true.
  */
using QueryTreeNodeToName = std::unordered_map<QueryTreeNodePtr, String>;
String calculateActionNodeName(const QueryTreeNodePtr & node,
    const PlannerContext & planner_context,
    QueryTreeNodeToName & node_to_name,
    bool use_column_identifier_as_action_node_name = true);

/** Calculate query tree expression node action dag name.
  *
  * For column node column node identifier from planner context is used, if use_column_identifier_as_action_node_name = true.
  */
String calculateActionNodeName(const QueryTreeNodePtr & node,
    const PlannerContext & planner_context,
    bool use_column_identifier_as_action_node_name = true);

/// Calculate action node name for constant
String calculateConstantActionNodeName(const Field & constant_literal, const DataTypePtr & constant_type);

/// Calculate action node name for constant, data type will be derived from constant literal value
String calculateConstantActionNodeName(const Field & constant_literal);

/** Calculate action node name for window node.
  * Window node action name can only be part of window function action name.
  * For column node column node identifier from planner context is used, if use_column_identifier_as_action_node_name = true.
  */
String calculateWindowNodeActionName(const QueryTreeNodePtr & function_node,
    const QueryTreeNodePtr & window_node,
    const PlannerContext & planner_context,
    bool use_column_identifier_as_action_node_name = true);

}
