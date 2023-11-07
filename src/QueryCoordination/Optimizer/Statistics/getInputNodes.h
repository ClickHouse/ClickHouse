#pragma once

#include <Interpreters/ActionsDAG.h>
#include <QueryCoordination/Optimizer/Statistics/ActionNodeVisitor.h>

namespace DB
{

struct Void
{
};

/// Get input nodes for an action node tree.
class InputNodeVisitor : public ActionNodeVisitor<ActionsDAG::NodeRawConstPtrs, Void>
{
public:
    using Base = ActionNodeVisitor<ActionsDAG::NodeRawConstPtrs, Void>;
    using VisitContext = Void;

    InputNodeVisitor() = default;

    ActionsDAG::NodeRawConstPtrs visit(const ActionsDAG::Node * node, ContextType & context) override;
    ActionsDAG::NodeRawConstPtrs visitChildren(const ActionsDAG::Node * node, ContextType & context) override;

    ActionsDAG::NodeRawConstPtrs visitInput(const ActionsDAG::Node * node, ContextType & context) override;
    ActionsDAG::NodeRawConstPtrs visitColumn(const ActionsDAG::Node * node, ContextType & context) override;
    ActionsDAG::NodeRawConstPtrs visitAlias(const ActionsDAG::Node * node, ContextType & context) override;
    ActionsDAG::NodeRawConstPtrs visitArrayJoin(const ActionsDAG::Node * node, ContextType & context) override;
    ActionsDAG::NodeRawConstPtrs visitFunction(const ActionsDAG::Node * node, ContextType & context);
};

ActionsDAG::NodeRawConstPtrs getInputNodes(const ActionsDAG::Node * node);

}
