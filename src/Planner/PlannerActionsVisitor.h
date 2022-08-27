#pragma once

#include <Common/HashTable/Hash.h>

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/SubqueryForSet.h>

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
  * 1. Column name to identifier map in planner context must be already initialized.
  * Identifiers in this map are used as action dag node names for column query tree nodes.
  * 2. Sets for IN functions are already collected in global context.
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
using QueryTreeNodeToName = std::unordered_map<const IQueryTreeNode *, String>;
String calculateActionsDAGNodeName(const IQueryTreeNode * node, const PlannerContext & planner_context, QueryTreeNodeToName & node_to_name);

/** Calculate query tree expression node name action dag name.
  *
  * For column node column node identifier from planner context is used.
  */
String calculateActionsDAGNodeName(const IQueryTreeNode * node, const PlannerContext & planner_context);


}
