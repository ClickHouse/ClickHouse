#include <QueryCoordination/Optimizer/Statistics/Utils.h>
#include <QueryCoordination/Optimizer/Statistics/ActionNodeVisitor.h>
#include <QueryCoordination/Optimizer/Statistics/ExpressionStatsCalculator.h>
#include <QueryCoordination/Optimizer/Statistics/getInputNodes.h>
#include <Common/logger_useful.h>

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
}

namespace DB
{

ActionNodeStatistics ExpressionNodeVisitor::visit(const ActionsDAGPtr actions_dag_ptr, ContextType & /*context*/)
{
    chassert(actions_dag_ptr != nullptr);
    chassert(!actions_dag_ptr->getOutputs().empty());

    /// TODO

    //    auto root_nodes = FindActionsRootNodes(actions_dag_ptr).get();
    //    for (auto node : root_nodes)
    //    {
    //        return visit(node, context);
    //    }
    return {};
}

ActionNodeStatistics ExpressionNodeVisitor::visit(const ActionsDAG::Node * node, ContextType & context)
{
    if (context.contains(node))
        return context[node];

    ActionNodeStatistics node_stats;
    switch (node->type)
    {
        case ActionsDAG::ActionType::INPUT:
            node_stats = visitInput(node, context);
            break;
        case ActionsDAG::ActionType::COLUMN:
            node_stats = visitColumn(node, context);
            break;
        case ActionsDAG::ActionType::ALIAS:
            node_stats = visitAlias(node, context);
            break;
        case ActionsDAG::ActionType::ARRAY_JOIN:
            node_stats = visitArrayJoin(node, context);
            break;
        case ActionsDAG::ActionType::FUNCTION:
            node_stats = visitFunction(node, context);
    }

    context.insert({node, node_stats});
    return node_stats;
}

ActionNodeStatistics ExpressionNodeVisitor::visitChildren(const ActionsDAG::Node * node, ContextType & context)
{
    chassert(node->children.size() == 1);
    return visit(node->children.front(), context);
}

ActionNodeStatistics ExpressionNodeVisitor::visitDefault(const ActionsDAG::Node * node, ContextType & context)
{
    auto input_nodes = getInputNodes(node);
    /// random(), now()
    chassert(!input_nodes.empty());

    ActionNodeStatistics node_stats;
    node_stats.selectivity = 1.0;

    for (auto input_node : input_nodes)
    {
        chassert(context.contains(input_node));
        node_stats.set(input_node, context[input_node].get(input_node)->clone());
    }

    return node_stats;
}

ActionNodeStatistics ExpressionNodeVisitor::visitInput(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "should never reach here.");
}

ActionNodeStatistics ExpressionNodeVisitor::visitColumn(const ActionsDAG::Node * node, ContextType & /*context*/)
{
    chassert(node->column);
    chassert(node->children.empty());

    if (!isColumnConst(*node->column))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} is not a constant column", node->column->getName());

    Float64 value;
    try
    {
        value = node->column->getFloat64(0);
    }
    catch (...)
    {
        LOG_TRACE(log, "Node {} can not be parsed to Float64", node->result_name);
        value = 0.0; // TODO use NaN
    }

    ActionNodeStatistics stats{.value = value};
    return stats;
}


ActionNodeStatistics ExpressionNodeVisitor::visitAlias(const ActionsDAG::Node * node, ContextType & context)
{
    ActionNodeStatistics stats = visitChildren(node, context);
    return stats;
}

ActionNodeStatistics ExpressionNodeVisitor::visitArrayJoin(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "method not implemented.");
}

ActionNodeStatistics ExpressionNodeVisitor::visitFunction(const ActionsDAG::Node * node, ContextType & context)
{
    switch (node->children.size())
    {
//        case 1:
//            return visitUnaryFunction(node, context);
//        case 2:
//            return visitBinaryFunction(node, context);
        default:
            return visitDefault(node, context);
    }
}

ActionNodeStatistics ExpressionNodeVisitor::visitUnaryFunction(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    ActionNodeStatistics r;
    return r;
}

ActionNodeStatistics ExpressionNodeVisitor::visitBinaryFunction(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    ActionNodeStatistics r;
    return r;
}


Statistics ExpressionStatsCalculator::calculateStatistics(const ActionsDAGPtr & expressions, const Statistics & input)
{
    auto & input_nodes = expressions->getInputs();
    ExpressionNodeVisitor::VisitContext context;

    /// check input contains all columns in input_nodes
    for (auto input_node : input_nodes)
    {
        chassert(input.getColumnStatisticsMap().contains(input_node->result_name));
    }

    ExpressionNodeVisitor visitor;
    ActionNodeStatistics node_stats = visitor.visit(expressions, context);

    Statistics statistics;
    statistics.setOutputRowSize(input.getOutputRowSize());

    return statistics;
}

ActionNodeStatistics
ExpressionStatsCalculator::calculateStatistics(const ActionsDAG::Node * node, ExpressionNodeVisitor::ContextType & context)
{
    ExpressionNodeVisitor visitor;
    return visitor.visit(node, context);
}

}
