#pragma once

#include <Common/HashTable/Hash.h>

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>

#include <Interpreters/Context_fwd.h>

#include <Analyzer/IQueryTreeNode.h>

#include <Interpreters/ActionsDAG.h>

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
  * is used as action dag node name.
  * 2. Sets for IN functions are already collected in planner global context.
  *
  * During actions build, there is special handling for following functions:
  * 1. Aggregate functions are added in actions dag as INPUT nodes. Aggregate functions arguments are not added.
  * 2. For function `in` and its variants, already collected sets from global context are used.
  */
class PlannerActionsVisitor
{
public:
    explicit PlannerActionsVisitor(const PlannerContextPtr & planner_context_);

    /** Add actions necessary to calculate expression node into expression dag.
      * Necessary actions are not added in actions dag output.
      * Returns query tree expression node actions dag nodes.
      */
    ActionsDAG::NodeRawConstPtrs visit(ActionsDAGPtr actions_dag, QueryTreeNodePtr expression_node);

private:
    const PlannerContextPtr planner_context;
};

/** Calculate query tree expression node name action dag name and add them into node to name map.
  * If node exists in map, name from map is used.
  *
  * For column node column node identifier from planner context is used.
  */
using QueryTreeNodeToName = std::unordered_map<QueryTreeNodePtr, String>;
String calculateActionNodeName(const QueryTreeNodePtr & node, const PlannerContext & planner_context, QueryTreeNodeToName & node_to_name);

/** Calculate query tree expression node action dag name.
  *
  * For column node column node identifier from planner context is used.
  */
String calculateActionNodeName(const QueryTreeNodePtr & node, const PlannerContext & planner_context);

/// Calculate action node name for constant
String calculateConstantActionNodeName(const Field & constant_literal, const DataTypePtr & constant_type);

/// Calculate action node name for constant, data type will be derived from constant literal value
String calculateConstantActionNodeName(const Field & constant_literal);

/** Calculate action node name for window node.
  * Window node action name can only be part of window function action name.
  */
String calculateWindowNodeActionName(const QueryTreeNodePtr & node, const PlannerContext & planner_context, QueryTreeNodeToName & node_to_name);

/** Calculate action node name for window node.
  * Window node action name can only be part of window function action name.
  */
String calculateWindowNodeActionName(const QueryTreeNodePtr & node, const PlannerContext & planner_context);

}
