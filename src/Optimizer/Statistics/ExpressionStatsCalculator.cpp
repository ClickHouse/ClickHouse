#include <Optimizer/Statistics/ExpressionStatsCalculator.h>
#include <Optimizer/Statistics/Utils.h>
#include <Optimizer/Statistics/getInputNodes.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
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

ActionNodeStatistics
ExpressionStatsCalculator::calculateStatistics(const ActionsDAG::Node * node, ExpressionNodeVisitor::ContextType & context)
{
    ExpressionNodeVisitor visitor;
    return visitor.visit(node, context);
}

Stats ExpressionStatsCalculator::calculateStatistics(const ActionsDAGPtr & expressions, const Stats & input)
{
    /// Expression (Before GROUP BY) maybe empty
    if (expressions->getNodes().empty())
        return input.clone();

    auto & input_nodes = expressions->getInputs();
    ExpressionNodeVisitor::VisitContext context;

    /// 1. init context
    for (auto input_node : input_nodes)
    {
        InputNodeStatsMap node_stats_map;
        /// input statistics contains all columns in input_nodes
        node_stats_map.insert({input_node, input.getColumnStatistics(input_node->result_name)});
        context.insert({input_node, {1.0, {}, node_stats_map}});
    }

    auto & output_nodes = expressions->getOutputs();

    /// There may be no output nodes, for example:
    /// select count(*) from hits as t1 join hits as t2 on t1.WatchID = t2.WatchID * 2 where t2.WatchID > 1

    /// 2. calculate output nodes statistics
    for (auto output_node : output_nodes)
        ExpressionStatsCalculator::calculateStatistics(output_node, context);

    Stats statistics;
    statistics.setOutputRowSize(input.getOutputRowSize());

    /// 3. add output node statistics to result
    for (auto output_node : output_nodes)
    {
        chassert(context.contains(output_node));

        /// Output column contains multi-input node, for example: 'col1 + col2'
        if (context[output_node].input_node_stats.size() > 1)
        {
            statistics.addColumnStatistics(output_node->result_name, ColumnStatistics::unknown());
        }
        /// for 'col1 + 1'
        else if (context[output_node].input_node_stats.size() == 1)
        {
            auto output_node_stats = context[output_node].get();
            statistics.addColumnStatistics(output_node->result_name, output_node_stats->clone());
        }
        /// for rand()
        else
        {
            statistics.addColumnStatistics(output_node->result_name, ColumnStatistics::create(*context[output_node].value));
        }
    }

    return statistics;
}

ActionNodeStatistics ExpressionNodeVisitor::visitChildren(const ActionsDAG::Node * node, ContextType & context)
{
    chassert(node->children.size() == 1);
    return visit(node->children.front(), context);
}

ActionNodeStatistics ExpressionNodeVisitor::visitDefault(const ActionsDAG::Node * node, ContextType & context)
{
    ActionNodeStatistics node_stats;
    node_stats.selectivity = 1.0;

    auto input_nodes = getInputNodes(node);
    if (input_nodes.empty())
    {
        /// For example: rand(), now(), materialize('str')
        node_stats.value = 0.0; /// TODO real value
        return node_stats;
    }

    for (auto input_node : input_nodes)
    {
        chassert(context.contains(input_node));
        auto input_stats = context[input_node].get(input_node)->clone();
        node_stats.set(input_node, input_stats);
        input_stats->setDataType(input_node->result_type);
    }
    return node_stats;
}

ActionNodeStatistics ExpressionNodeVisitor::visitInput(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Should never reach here, for we can get input node stats from context.");
}

ActionNodeStatistics ExpressionNodeVisitor::visitColumn(const ActionsDAG::Node * node, ContextType & /*context*/)
{
    chassert(node->column);
    chassert(node->children.empty());

    if (!isColumnConst(*node->column))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} is not a constant column", node->column->getName());

    ActionNodeStatistics node_stats;

    if (isNumeric(node->result_type))
        node_stats.value = node->column->getFloat64(0);
    else
        node_stats.value = 0.0;

    LOG_TRACE(log, "visit column {} got {}", node->result_name, *node_stats.value);
    return node_stats;
}

ActionNodeStatistics ExpressionNodeVisitor::visitAlias(const ActionsDAG::Node * node, ContextType & context)
{
    ActionNodeStatistics stats = visitChildren(node, context);
    return stats;
}

ActionNodeStatistics ExpressionNodeVisitor::visitArrayJoin(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    /// TODO implement
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "method not implemented.");
}

ActionNodeStatistics ExpressionNodeVisitor::visitFunction(const ActionsDAG::Node * node, ContextType & context)
{
    switch (node->children.size())
    {
        case 1:
            return visitUnaryFunction(node, context);
        case 2:
            return visitBinaryFunction(node, context);
        default:
            return visitDefault(node, context);
    }
}

ActionNodeStatistics ExpressionNodeVisitor::visitUnaryFunction(const ActionsDAG::Node * node, ContextType & context)
{
    /// TODO implement
    return visitDefault(node, context);
}

ActionNodeStatistics ExpressionNodeVisitor::visitBinaryFunction(const ActionsDAG::Node * node, ContextType & context)
{
    /// TODO implement
    return visitDefault(node, context);
}

}
