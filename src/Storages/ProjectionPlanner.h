#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Core/Block.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/VirtualColumnsDescription.h>

namespace DB
{

/** A small planner for the `SELECT` of a projection.
  *
  * A projection stores its columns under the names of the expressions of its query as they are
  * written (see `ProjectionDescription::sample_block`): those names are the names of the files in a
  * projection part. So the actions built here name their nodes that way, not the way `Planner` does
  * (`__table1.a`, `1_UInt8`). A name is taken from the query tree before the analysis resolves it,
  * because resolving a function renames it (`sum` of `NULL` becomes `nothingNull`, `count() IGNORE NULLS`
  * becomes `count`), and from the resolved tree for the nodes which the analysis creates (columns,
  * folded constants).
  */
class ProjectionPlanner
{
public:
    /// `query_ast` is the `SELECT` of the projection, which has no `FROM`: it reads `source_columns`.
    ProjectionPlanner(
        const ASTPtr & query_ast,
        const ColumnsDescription & source_columns,
        VirtualColumnsDescription virtuals,
        const ContextPtr & context_);

    bool isAggregate() const { return is_aggregate; }

    /// The context the query is analyzed with.
    const ContextPtr & getContext() const { return context; }

    const Names & getAggregationKeys() const { return aggregation_keys; }
    const AggregateDescriptions & getAggregates() const { return aggregates; }

    /// Actions which calculate the aggregation keys and the arguments of the aggregate functions from the source columns.
    ActionsDAG buildBeforeAggregationActions() const;

private:
    void collectNodeNames(const QueryTreeNodePtr & node, const QueryTreeNodeToName & unresolved_node_names);

    ContextPtr context;
    QueryTreeNodePtr query_tree;
    PlannerContextPtr planner_context;
    /// The names the projection stores the expressions of its query under.
    QueryTreeNodeToName node_to_name;
    bool is_aggregate = false;
    Names aggregation_keys;
    AggregateDescriptions aggregates;
};

/// Fake storage which stands in for the missing `FROM` clause of a projection query: provides `columns` and reads `data`.
StoragePtr createProjectionQuerySource(const ColumnsDescription & columns, VirtualColumnsDescription virtuals, Block data = {});

/// Builds the query tree of the `SELECT` of a projection over `source` and runs the analysis passes on it.
/// If `unresolved_node_names` is set, remembers the names of the functions before the analysis.
QueryTreeNodePtr buildProjectionQueryTree(
    const ASTPtr & query_ast,
    const StoragePtr & source,
    const ContextPtr & context,
    QueryTreeNodeToName * unresolved_node_names = nullptr);

}
