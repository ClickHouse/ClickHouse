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

class PlannerActionsVisitor
{
public:
    explicit PlannerActionsVisitor(ActionsDAGPtr actions_dag, const PlannerContextPtr & planner_context_);

    ActionsDAG::NodeRawConstPtrs visit(QueryTreeNodePtr expression_node);

    ~PlannerActionsVisitor();

private:
    using NodeNameAndNodeMinLevel = std::pair<std::string, size_t>;

    NodeNameAndNodeMinLevel visitImpl(QueryTreeNodePtr node);

    NodeNameAndNodeMinLevel visitColumn(const QueryTreeNodePtr & node);

    NodeNameAndNodeMinLevel visitConstant(const QueryTreeNodePtr & node);

    NodeNameAndNodeMinLevel visitLambda(const QueryTreeNodePtr & node);

    NodeNameAndNodeMinLevel visitFunction(const QueryTreeNodePtr & node);

    String getActionsDAGNodeName(const IQueryTreeNode * node) const;

    class ActionsScopeNode;
    std::vector<std::unique_ptr<ActionsScopeNode>> actions_stack;
    const PlannerContextPtr planner_context;
};

}
