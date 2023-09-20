#include <QueryCoordination/Optimizer/Statistics/ActionNodeStatistics.h>
#include <QueryCoordination/Optimizer/Statistics/ActionNodeVisitor.h>
#include <QueryCoordination/Optimizer/Statistics/ExpressionStatsCalculator.h>
#include <QueryCoordination/Optimizer/Statistics/PredicateStatsCalculator.h>
#include <QueryCoordination/Optimizer/Statistics/Utils.h>
#include <QueryCoordination/Optimizer/Statistics/getInputNodes.h>


namespace
{

/// find action node by node result name
DB::ActionsDAG::NodeRawConstPtrs findNodesByColumns(const DB::Names & names, DB::PredicateNodeVisitor::VisitContext context)
{
    DB::ActionsDAG::NodeRawConstPtrs nodes;
    for (const auto & name : names)
    {
        for (auto & entry : context)
        {
            if (entry.first->result_name == name)
                nodes.push_back(entry.first);
        }
        chassert(nodes.size() > 0 && nodes.back()->result_name == name);
    }
    return nodes;
}

}

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

ActionNodeStatistics PredicateNodeVisitor::visit(const ActionsDAG::Node * node, ContextType & context)
{
    if (context.contains(node))
        return context[node];
    
    auto node_stats = Base::visit(node, context);
    context.insert({node, node_stats});

    return node_stats;
}

ActionNodeStatistics PredicateNodeVisitor::visitChildren(const ActionsDAG::Node * node, ContextType & context)
{
    chassert(node->children.size() == 1);
    return visit(node->children.front(), context); /// TODO clone
}

ActionNodeStatistics PredicateNodeVisitor::visitInput(const ActionsDAG::Node * node, ContextType & context)
{
    return ExpressionStatsCalculator::calculateStatistics(node, context);
}

ActionNodeStatistics PredicateNodeVisitor::visitColumn(const ActionsDAG::Node * node, ContextType & context)
{
    return ExpressionStatsCalculator::calculateStatistics(node, context);
}


ActionNodeStatistics PredicateNodeVisitor::visitAlias(const ActionsDAG::Node * node, ContextType & context)
{
    return visitChildren(node, context);
}

ActionNodeStatistics PredicateNodeVisitor::visitArrayJoin(const ActionsDAG::Node * node, ContextType & context)
{
    return ExpressionStatsCalculator::calculateStatistics(node, context);
}

ActionNodeStatistics PredicateNodeVisitor::visitAnd(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    ActionNodeStatistics r;
    return r;
}

ActionNodeStatistics PredicateNodeVisitor::visitOr(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    ActionNodeStatistics r;
    return r;
}

ActionNodeStatistics PredicateNodeVisitor::visitNot(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    ActionNodeStatistics r;
    return r;
}

ActionNodeStatistics PredicateNodeVisitor::visitIn(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    ActionNodeStatistics r;
    return r;
}

ActionNodeStatistics PredicateNodeVisitor::visitEqual(const ActionsDAG::Node * node, ContextType & context)
{
    chassert(node->children.size() == 2);

    ActionNodeStatistics node_stats;

    auto & left = node->children[0];
    auto & right = node->children[1];

    /// 1 = 2
    if (isConstColumn(left) && isConstColumn(right))
    {

    }
    /// col = 1
    else if (!isConstColumn(left) && isConstColumn(right))
    {
        auto left_stats = ExpressionStatsCalculator::calculateStatistics(left, context);
        auto right_stats = ExpressionStatsCalculator::calculateStatistics(right, context);

        if (isNumeric(left->result_type))
        {
            /// get unique left input node stats
            auto input_nodes = getInputNodes(left);
            chassert(input_nodes.size() == 1);
            auto uniq_input_node_stats = left_stats.get(input_nodes[0]);
            chassert(uniq_input_node_stats);

            /// right stats is constant
            chassert(right_stats.value.has_value());

            /// calculate selectivity
            auto cloned = uniq_input_node_stats->clone();
            auto selectivity = cloned->calculateSelectivity(ColumnStatistics::EQUAL, *right_stats.value);

            /// update min_value or max_value
            cloned->updateValues(ColumnStatistics::EQUAL, *right_stats.value);

            node_stats.selectivity = selectivity;
            node_stats.input_node_stats[input_nodes[0]] = cloned;
        }
        else
        {
            /// TODO
        }
    }
    /// 1 = col
    else if (isConstColumn(left) && !isConstColumn(right))
    {

    }
    /// col1 = col2
    else if (!isConstColumn(left) && !isConstColumn(right))
    {

    }

    return node_stats;
}

ActionNodeStatistics PredicateNodeVisitor::visitNotEqual(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    ActionNodeStatistics r;
    return r;
}

ActionNodeStatistics PredicateNodeVisitor::visitGreater(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    ActionNodeStatistics r;
    return r;
}

ActionNodeStatistics PredicateNodeVisitor::visitGreaterOrEqual(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    ActionNodeStatistics r;
    return r;
}

ActionNodeStatistics PredicateNodeVisitor::visitLess(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    ActionNodeStatistics r;
    return r;
}

ActionNodeStatistics PredicateNodeVisitor::visitLessOrEqual(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    ActionNodeStatistics r;
    return r;
}

ActionNodeStatistics PredicateNodeVisitor::visitOtherFuncs(const ActionsDAG::Node * node, ContextType & context)
{
    ActionNodeStatistics node_stats;
    auto input_nodes = getInputNodes(node);
    node_stats.selectivity = 0.5; /// TODO add to settings

    /// collect input node statistics
    for (auto input_node : input_nodes)
    {
        auto cloned = context[input_node].get(input_node)->clone();
        node_stats.set(input_node, cloned);
    }
    return node_stats;
}

Statistics PredicateStatsCalculator::calculateStatistics(
    const ActionsDAGPtr & predicates, const String & filter_node_name, const Statistics & input, const Names & output_columns)
{
    Statistics statistics;

    auto & input_nodes = predicates->getInputs();
    PredicateNodeVisitor::VisitContext context;

    /// 1. init context
    for (auto input_node : input_nodes)
    {
        InputNodeStatsMap node_stats_map;
        /// input statistics contains all columns in input_nodes
        node_stats_map.insert({input_node, input.getColumnStatistics(input_node->result_name)->clone()});
        context.insert({input_node, {1.0, {}, node_stats_map}});
    }

    /// 2. calculate filter node
    PredicateNodeVisitor visitor;
    const auto * filter_node = &predicates->findInOutputs(filter_node_name);
    ActionNodeStatistics filter_node_stats = visitor.visit(filter_node, context);
    statistics.addColumnStatistics(filter_node->result_name, std::make_shared<ColumnStatistics>(0.0, 1.0, 2.0, 1.0));

    auto & output_nodes = predicates->getOutputs();
    chassert(output_nodes.size() > 0);

    /// 3. calculate other output nodes
    for (const auto & output_node : output_nodes)
    {
        if (output_node != filter_node)
            ExpressionStatsCalculator::calculateStatistics(output_node, context);
    }

    statistics.setOutputRowSize(filter_node_stats.selectivity * input.getOutputRowSize());

    /// 4. adjust output node statistics
    auto real_output_columns = output_columns.empty() ? output_nodes : findNodesByColumns(output_columns, context);

    for (const auto & output_node : real_output_columns)
    {
        if (output_node == filter_node)
            continue;

        chassert(context.contains(output_node));

        /// Output column contains multi-input node, for example: 'col1 + col2'
        if (context[output_node].input_node_stats.size() > 1)
        {
            statistics.addColumnStatistics(output_node->result_name, ColumnStatistics::unknown());
        }
        else
        {
            auto output_node_stats = context[output_node].get();
            chassert(output_node_stats);
            statistics.addColumnStatistics(output_node->result_name, output_node_stats->clone());
        }
    }

    statistics.adjustStatistics();
    return statistics;
}

}
