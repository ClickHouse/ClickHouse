#include <Optimizer/Statistics/getInputNodes.h>

namespace DB
{

ActionsDAG::NodeRawConstPtrs InputNodeVisitor::visit(const ActionsDAG::Node * node, ContextType & context)
{
    ActionsDAG::NodeRawConstPtrs nodes;
    switch (node->type)
    {
        case ActionsDAG::ActionType::INPUT:
            nodes = visitInput(node, context);
            break;
        case ActionsDAG::ActionType::COLUMN:
            nodes = visitColumn(node, context);
            break;
        case ActionsDAG::ActionType::ALIAS:
            nodes = visitAlias(node, context);
            break;
        case ActionsDAG::ActionType::ARRAY_JOIN:
            nodes = visitArrayJoin(node, context);
            break;
        case ActionsDAG::ActionType::FUNCTION:
            nodes = visitFunction(node, context);
    }
    return nodes;
}

ActionsDAG::NodeRawConstPtrs InputNodeVisitor::visitChildren(const ActionsDAG::Node * node, ContextType & context)
{
    ActionsDAG::NodeRawConstPtrs ret;
    for (auto child : node->children)
    {
        auto child_ret = visit(child, context);
        ret.insert(ret.end(), child_ret.begin(), child_ret.end());
    }
    return ret;
}

ActionsDAG::NodeRawConstPtrs InputNodeVisitor::visitInput(const ActionsDAG::Node * node, ContextType &)
{
    return {node};
}

ActionsDAG::NodeRawConstPtrs InputNodeVisitor::visitColumn(const ActionsDAG::Node *, ContextType &)
{
    return {};
}

ActionsDAG::NodeRawConstPtrs InputNodeVisitor::visitAlias(const ActionsDAG::Node * node, ContextType & context)
{
    return visitChildren(node, context);
}

ActionsDAG::NodeRawConstPtrs InputNodeVisitor::visitArrayJoin(const ActionsDAG::Node *, ContextType &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "method not implemented");
}

ActionsDAG::NodeRawConstPtrs InputNodeVisitor::visitFunction(const ActionsDAG::Node * node, ContextType & context)
{
    return visitChildren(node, context);
}

ActionsDAG::NodeRawConstPtrs getInputNodes(const ActionsDAG::Node * node)
{
    InputNodeVisitor::VisitContext context;
    return InputNodeVisitor().visit(node, context);
}

}
