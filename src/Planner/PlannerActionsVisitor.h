#pragma once

#include <Core/Names.h>

#include <Interpreters/Context_fwd.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/IQueryTreeNode.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/WindowDescription.h>

namespace DB
{

struct CorrelatedSubtrees;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

class ColumnNode;
using ColumnNodePtr = std::shared_ptr<ColumnNode>;
using ColumnNodes = std::vector<ColumnNodePtr>;

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
    explicit PlannerActionsVisitor(
      const PlannerContextPtr & planner_context_,
      const ColumnNodePtrWithHashSet & correlated_columns_set_,
      bool use_column_identifier_as_action_node_name_ = true);

    /** Add actions necessary to calculate expression node into expression dag.
      * Necessary actions are not added in actions dag output.
      * Returns query tree expression node actions dag nodes.
      */
    std::pair<ActionsDAG::NodeRawConstPtrs, CorrelatedSubtrees> visit(ActionsDAG & actions_dag, QueryTreeNodePtr expression_node);

private:
    const PlannerContextPtr planner_context;
    const ColumnNodePtrWithHashSet & correlated_columns_set;
    bool use_column_identifier_as_action_node_name = true;
};

/** Spelling of a plan column identifier inside an action node name.
  *
  * Servers before 26.5 concatenated the source alias and the column name without quoting them.
  * `Legacy` reproduces that spelling; it is only ever used to render names a pre-26.5 peer
  * exchanges over the wire. Everything inside the planner uses `Current`.
  */
enum class ColumnIdentifierConvention : uint8_t
{
    Current,
    Legacy,
};

struct RenderedColumnIdentifier
{
    enum class State : uint8_t
    {
        /// The node has no registered plan column identifier, so its bare column name names it.
        NoIdentifier,
        /// `name` holds the identifier under the requested convention.
        Rendered,
        /// The node has a registered identifier that cannot be regenerated from the node's current
        /// parts, so its spelling under the requested convention is unknown.
        Unrenderable,
    };

    State state = State::NoIdentifier;
    /// Set for State::Rendered only.
    String name;
};

/** Render the plan column identifier of a column node under `convention`.
  *
  * The identifier is generated from the column and its source, never by rewriting an already
  * rendered name: a rendered name cannot be parsed back unambiguously.
  */
RenderedColumnIdentifier renderColumnNodeIdentifier(const QueryTreeNodePtr & column_node,
    const PlannerContext & planner_context,
    ColumnIdentifierConvention convention);

/** Calculate query tree expression node action dag name and add them into node to name map.
  * If node exists in map, name from map is used.
  *
  * For column node column node identifier from planner context is used, if use_column_identifier_as_action_node_name = true.
  *
  * When `unrenderable_identifier_nodes` is given, every node whose name was built over a column
  * that has a registered plan column identifier which cannot be rendered under
  * `column_identifier_convention` is added to it: such a name does not identify the column. It must
  * be passed together with the same `node_to_name` on every call, since a name answered from that
  * cache is rendered only once.
  */
using QueryTreeNodeToName = std::unordered_map<QueryTreeNodePtr, String>;
using QueryTreeNodeSet = std::unordered_set<QueryTreeNodePtr>;
String calculateActionNodeName(const QueryTreeNodePtr & node,
    const PlannerContext & planner_context,
    QueryTreeNodeToName & node_to_name,
    bool use_column_identifier_as_action_node_name = true,
    ColumnIdentifierConvention column_identifier_convention = ColumnIdentifierConvention::Current,
    QueryTreeNodeSet * unrenderable_identifier_nodes = nullptr);

/** Calculate query tree expression node action dag name.
  *
  * For column node column node identifier from planner context is used, if use_column_identifier_as_action_node_name = true.
  */
String calculateActionNodeName(const QueryTreeNodePtr & node,
    const PlannerContext & planner_context,
    bool use_column_identifier_as_action_node_name = true,
    ColumnIdentifierConvention column_identifier_convention = ColumnIdentifierConvention::Current);

/// Calculate action node name for constant
String calculateConstantActionNodeName(const Field & constant_literal, const DataTypePtr & constant_type);

String calculateConstantActionNodeName(const ConstantNode & constant_node, Int64 optimize_const_name_size);

/// Calculate action node name for constant, data type will be derived from constant literal value
String calculateConstantActionNodeName(const Field & constant_literal);

/** Calculate action node name for window node.
  * Window node action name can only be part of window function action name.
  * For column node column node identifier from planner context is used, if use_column_identifier_as_action_node_name = true.
  */
String calculateWindowNodeActionName(const QueryTreeNodePtr & window_node,
    const PlannerContext & planner_context,
    bool use_column_identifier_as_action_node_name = true);

}
