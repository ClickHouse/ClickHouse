#include <Columns/ColumnConst.h>
#include <QueryCoordination/Optimizer/Statistics/ActionNodeStatistics.h>
#include <QueryCoordination/Optimizer/Statistics/ActionNodeVisitor.h>
#include <QueryCoordination/Optimizer/Statistics/ExpressionStatsCalculator.h>
#include <QueryCoordination/Optimizer/Statistics/PredicateStatsCalculator.h>
#include <QueryCoordination/Optimizer/Statistics/Utils.h>
#include <QueryCoordination/Optimizer/Statistics/getInputNodes.h>
#include <Common/logger_useful.h>

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
}

namespace DB
{

class PredicateNodeVisitor : public ActionNodeVisitor<ActionNodeStatistics, std::unordered_map<const ActionsDAG::Node *, ActionNodeStatistics>>
{
public:
    using Base = ActionNodeVisitor<ActionNodeStatistics, std::unordered_map<const ActionsDAG::Node *, ActionNodeStatistics>>;
    using VisitContext = std::unordered_map<const ActionsDAG::Node *, ActionNodeStatistics>;

    PredicateNodeVisitor() : log(&Poco::Logger::get("PredicateNodeVisitor")) {}

    ActionNodeStatistics visit(const ActionsDAGPtr actions_dag_ptr, ContextType & context) override;
    ActionNodeStatistics visit(const ActionsDAG::Node * node, ContextType & context) override;

    ActionNodeStatistics visitChildren(const ActionsDAG::Node * node, ContextType & context) override;

    ActionNodeStatistics visitInput(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitColumn(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitAlias(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitArrayJoin(const ActionsDAG::Node * node, ContextType & context) override;

    /// functions
    ActionNodeStatistics visitAnd(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitOr(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitNot(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitIn(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitEqual(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitNotEqual(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitGreater(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitGreaterOrEqual(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitLess(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitLessOrEqual(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitOtherFuncs(const ActionsDAG::Node * node, ContextType & context) override;

private:
    Poco::Logger * log;
};

ActionNodeStatistics PredicateNodeVisitor::visit(const ActionsDAGPtr /*actions_dag_ptr*/, ContextType & /*context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "method not implemented");
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
    node_stats.selectivity = 0.8; /// TODO add to settings

    /// collect input node statistics
    for (auto input_node : input_nodes)
    {
        auto cloned = context[input_node].get(input_node)->clone();
        node_stats.set(input_node, cloned);
    }
    return node_stats;
}

Statistics PredicateStatsCalculator::calculateStatistics(const ActionsDAGPtr & predicates, const String & filter_node_name, const Statistics & input)
{
    Statistics statistics;

    auto & input_nodes = predicates->getInputs();
    PredicateNodeVisitor::VisitContext context;

    /// 1. init context
    for (auto input_node : input_nodes)
    {
        /// check input contains all columns in input_nodes
        chassert(input.getColumnStatisticsMap().contains(input_node->result_name));
        InputNodeStatsMap node_stats_map;

        node_stats_map.insert({input_node, input.getColumnStatistics(input_node->result_name)});
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
    if (output_nodes.size() > 1)
    {
        for (size_t i = 1; i < output_nodes.size(); i++)
            ExpressionStatsCalculator::calculateStatistics(output_nodes[i], context);
    }

    statistics.setOutputRowSize(filter_node_stats.selectivity * input.getOutputRowSize());

    /// 4. adjust output node statistics
    for (size_t i = 1; i < output_nodes.size(); i++)
    {
        chassert(context.contains(output_nodes[i]));
        chassert(context[output_nodes[i]].input_node_stats.size() == 1);  /// TODO support 'col1 + col2'

        auto output_node_stats = context[output_nodes[i]].get();
        chassert(output_node_stats);

//        /// Next step will not use alias as input node, but its child.
//        if (output_nodes[i]->type == ActionsDAG::ActionType::ALIAS)
//            statistics.addColumnStatistics(output_nodes[i]->children[0]->result_name, output_node_stats);
//        else
        statistics.addColumnStatistics(output_nodes[i]->result_name, output_node_stats);

        adjustActionNodeStats(statistics.getOutputRowSize(), output_node_stats);
    }
    return statistics;
}

}
