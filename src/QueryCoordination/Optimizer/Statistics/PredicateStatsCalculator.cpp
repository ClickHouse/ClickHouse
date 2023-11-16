#include <QueryCoordination/Optimizer/Statistics/ActionNodeStatistics.h>

#include <Columns/ColumnSet.h>
#include <Interpreters/Set.h>
#include <QueryCoordination/Optimizer/Statistics/ActionNodeVisitor.h>
#include <QueryCoordination/Optimizer/Statistics/ExpressionStatsCalculator.h>
#include <QueryCoordination/Optimizer/Statistics/PredicateStatsCalculator.h>
#include <QueryCoordination/Optimizer/Statistics/Utils.h>
#include <QueryCoordination/Optimizer/Statistics/getInputNodes.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
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
        cloned->setDataType(node->result_type);
    }
    return node_stats;
}

Statistics
PredicateStatsCalculator::calculateStatistics(const ActionsDAGPtr & predicates, const String & filter_node_name, const Statistics & input)
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
        if (output_node != filter_node)
            ExpressionStatsCalculator::calculateStatistics(output_node, context);

    statistics.setOutputRowSize(filter_node_stats.selectivity * input.getOutputRowSize());

    /// 4. adjust output node statistics
    for (const auto & output_node : output_nodes)
    {
        if (output_node == filter_node)
            continue;

        /// we should use node in filter_node tree first, for we modify the node statistics in it.
        /// For example select a, b from t where a > 1, we should get a from filter_node_stats and
        /// b from context.
        if (auto column_in_filter = filter_node_stats.get(output_node))
            statistics.addColumnStatistics(output_node->result_name, column_in_filter->clone());
        else
        {
            chassert(context.contains(output_node));

            /// Output column contains multi-input node, for example: 'col1 + col2'
            if (context[output_node].input_node_stats.size() > 1)
            {
                statistics.addColumnStatistics(output_node->result_name, ColumnStatistics::unknown());
            }
            else
            {
                auto output_node_stats = context[output_node].get();
                chassert(output_node_stats != nullptr);
                statistics.addColumnStatistics(output_node->result_name, output_node_stats->clone());
            }
        }
    }

    statistics.adjustStatistics();
    return statistics;
}

ActionNodeStatistics PredicateNodeVisitor::visit(const ActionsDAG::Node * node, ContextType & context)
{
    if (context.contains(node))
        return context[node];

    auto node_stats = Base::visit(node, context);
    context.insert({node, node_stats});

    return node_stats;
}

ActionNodeStatistics PredicateNodeVisitor::visitChildren(const ActionsDAG::Node *, ContextType &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "method not implemented");
}

ActionNodeStatistics PredicateNodeVisitor::visitInput(const ActionsDAG::Node * node, ContextType & context)
{
    return ExpressionStatsCalculator::calculateStatistics(node, context);
}

ActionNodeStatistics PredicateNodeVisitor::visitColumn(const ActionsDAG::Node * node, ContextType & context)
{
    return ExpressionStatsCalculator::calculateStatistics(node, context);
}

ActionNodeStatistics PredicateNodeVisitor::visitAnd(const ActionsDAG::Node * node, ContextType & context)
{
    ActionNodeStatistics node_stats;

    auto & left = node->children[0];
    auto & right = node->children[1];

    auto left_stats = visit(left, context);
    auto right_stats = visit(right, context);

    auto left_input_nodes = left_stats.getInputNodes();
    auto right_input_nodes = right_stats.getInputNodes();

    std::set<const ActionsDAG::Node *> all_input_nodes;
    all_input_nodes.insert(left_input_nodes.begin(), left_input_nodes.end());
    all_input_nodes.insert(right_input_nodes.begin(), right_input_nodes.end());

    for (auto input_node : all_input_nodes)
    {
        if (left_input_nodes.contains(input_node) && right_input_nodes.contains(input_node))
        {
            /// merge left_input_node_stat & left_input_node_stat
            auto left_input_node_stat = left_stats.get(input_node);
            auto right_input_node_stat = right_stats.get(input_node);

            auto input_node_stats = left_input_node_stat->clone();
            input_node_stats->mergeColumnValueByIntersect(right_input_node_stat);
            input_node_stats->setNdv(std::max(1.0, std::min(left_input_node_stat->getNdv(), right_input_node_stat->getNdv())));
            node_stats.set(input_node, input_node_stats);
        }
        else if (left_input_nodes.contains(input_node))
        {
            auto left_input_node_stat = left_stats.get(input_node);
            auto input_node_stats = left_input_node_stat->clone();
            node_stats.set(input_node, input_node_stats);
        }
        else
        {
            auto right_input_node_stat = right_stats.get(input_node);
            auto input_node_stats = right_input_node_stat->clone();
            node_stats.set(input_node, input_node_stats);
        }
    }

    /// For col > 1 and col < 8, selectivity = left_selectivity * right_selectivity.
    /// TODO For col > 1 and col > 2, selectivity may be smaller than the real one, should calculate by range.
    node_stats.selectivity = left_stats.selectivity * right_stats.selectivity;

    return node_stats;
}

ActionNodeStatistics PredicateNodeVisitor::visitOr(const ActionsDAG::Node * node, ContextType & context)
{
    ActionNodeStatistics node_stats;

    auto & left = node->children[0];
    auto & right = node->children[1];

    auto left_stats = visit(left, context);
    auto right_stats = visit(right, context);

    auto left_input_nodes = left_stats.getInputNodes();
    auto right_input_nodes = right_stats.getInputNodes();

    std::set<const ActionsDAG::Node *> all_input_nodes;
    all_input_nodes.insert(left_input_nodes.begin(), left_input_nodes.end());
    all_input_nodes.insert(right_input_nodes.begin(), right_input_nodes.end());

    Float64 selectivity = left_stats.selectivity + right_stats.selectivity - left_stats.selectivity * right_stats.selectivity;
    node_stats.selectivity = std::min(1.0, selectivity);

    for (auto input_node : all_input_nodes)
    {
        if (left_input_nodes.contains(input_node) && right_input_nodes.contains(input_node))
        {
            /// merge left_input_node_stat & left_input_node_stat
            auto left_input_node_stat = left_stats.get(input_node);
            auto right_input_node_stat = right_stats.get(input_node);


            auto input_node_stats = left_input_node_stat->clone();
            input_node_stats->mergeColumnValueByUnion(right_input_node_stat);

            Float64 ndv = (left_input_node_stat->getNdv() + right_input_node_stat->getNdv()) * selectivity;
            input_node_stats->setNdv(std::max(1.0, ndv));
            node_stats.set(input_node, input_node_stats);
        }
        else if (left_input_nodes.contains(input_node))
        {
            auto left_input_node_stat = left_stats.get(input_node);
            auto input_node_stats = left_input_node_stat->clone();
            node_stats.set(input_node, input_node_stats);
        }
        else
        {
            auto right_input_node_stat = right_stats.get(input_node);
            auto input_node_stats = right_input_node_stat->clone();
            node_stats.set(input_node, input_node_stats);
        }
    }

    return node_stats;
}

ActionNodeStatistics PredicateNodeVisitor::visitNot(const ActionsDAG::Node * node, ContextType & context)
{
    ActionNodeStatistics node_stats;

    auto & child = node->children[0];
    auto child_stats = visit(child, context);

    Float64 selectivity = 1.0 - child_stats.selectivity;
    node_stats.selectivity = std::min(1.0, selectivity);

    auto child_input_nodes = child_stats.getInputNodes();

    for (auto child_input_node : child_input_nodes)
    {
        auto child_input_node_stat = child_stats.get(child_input_node)->clone();
        node_stats.set(child_input_node, child_input_node_stat);
    }

    return node_stats;
}

ActionNodeStatistics PredicateNodeVisitor::visitIn(const ActionsDAG::Node * node, ContextType & context)
{
    ActionNodeStatistics node_stats;

    auto & left = node->children[0];
    auto & set = node->children[1];

    auto left_stats = visit(left, context);
    auto uniq_left_stats = left_stats.get();

    chassert(uniq_left_stats);
    uniq_left_stats = uniq_left_stats->clone();

    chassert(set->column != nullptr);
    Float64 num_value_in_column{};

    if (uniq_left_stats->isUnKnown())
    {
        node_stats.set(left, uniq_left_stats);
        node_stats.selectivity = 0.1; /// TODO add to settings
        return node_stats;
    }

    ColumnPtr set_data_column;

    /// For constant set: in (1, 2)
    if (auto column_const = dynamic_cast<const ColumnConst *>(set->column.get()))
    {
        chassert(column_const->getDataColumnPtr() != nullptr);
        if (const auto * column_set = dynamic_cast<const ColumnSet *>(column_const->getDataColumnPtr().get()))
        {
            chassert(column_set->getData() != nullptr);
            auto set_ptr = column_set->getData()->get();
            if (set_ptr && set_ptr->hasExplicitSetElements()) /// TODO set not filled in prewhere
                set_data_column = set_ptr->getSetElements()[0]; /// TODO multi-column
        }
    }
    /// For subquery set: in (select * from t)
    else if (const auto * column_set = dynamic_cast<const ColumnSet *>(set->column.get()))
    {
        chassert(column_set->getData() != nullptr);
        auto set_ptr = column_set->getData()->get();
        if (set_ptr)
            set_data_column = set_ptr->getSetElements()[0];
    }

    if (set_data_column)
    {
        Float64 min_value{}, max_value{};
        bool find_min_value{}, find_max_value{};

        if (isNumeric(left->result_type))
        {
            for (size_t i = 0; i < set_data_column->size(); i++)
            {
                auto value = set_data_column->getFloat64(i);
                if (uniq_left_stats->inRange(value))
                {
                    num_value_in_column++;
                    if (!find_min_value)
                    {
                        min_value = value;
                        find_min_value = true;
                    }
                    if (i == set_data_column->size() - 1)
                        max_value = value;
                }
                else if (find_min_value && !find_max_value)
                {
                    max_value = set_data_column->getFloat64(i - 1);
                    find_max_value = true;
                }
            }
        }
        else
            num_value_in_column = set_data_column->size();

        uniq_left_stats->setNdv(num_value_in_column);
        if (find_min_value)
            uniq_left_stats->setMinValue(min_value);
        if (find_max_value)
            uniq_left_stats->setMaxValue(max_value);
    }
    /// subquery not executed yet
    else
    {
        /// filtering happens later at CreatingSetsStep
        num_value_in_column = uniq_left_stats->getNdv();
    }

    node_stats.set(left, uniq_left_stats);
    node_stats.selectivity = num_value_in_column / uniq_left_stats->getNdv() * 0.8; /// TODO add to settings

    return node_stats;
}

ActionNodeStatistics PredicateNodeVisitor::calculateBinaryPredicateFunction(
    const ActionsDAG::Node * node, ColumnStatistics::OP_TYPE op_type, ContextType & context)
{
    chassert(node->children.size() == 2);

    ActionNodeStatistics node_stats;

    auto & left = node->children[0];
    auto & right = node->children[1];

    auto left_stats = ExpressionStatsCalculator::calculateStatistics(left, context);
    auto right_stats = ExpressionStatsCalculator::calculateStatistics(right, context);

    auto handle_variable_and_const = [&node_stats, &op_type](
                                         ActionNodeStatistics & var_stats,
                                         const ActionsDAG::Node * const & var_node,
                                         ActionNodeStatistics & const_stats,
                                         const ActionsDAG::Node * const & /*const_node*/)
    {
        /// get unique variable input node stats
        auto input_nodes = getInputNodes(var_node);
        chassert(input_nodes.size() == 1);
        auto uniq_input_node_stats = var_stats.get(input_nodes[0]);
        chassert(uniq_input_node_stats);

        /// calculate selectivity
        auto cloned = uniq_input_node_stats->clone();
        if (cloned->isUnKnown())
            node_stats.selectivity = 0.1; /// TODO add to settings
        else
            node_stats.selectivity = cloned->calculateByValue(op_type, *const_stats.value);
        node_stats.input_node_stats[input_nodes[0]] = cloned;
    };

    /// col = 1
    if (!isConstColumn(left) && isConstColumn(right))
    {
        handle_variable_and_const(left_stats, left, right_stats, right);
    }
    /// 1 = col
    else if (isConstColumn(left) && !isConstColumn(right))
    {
        handle_variable_and_const(right_stats, right, left_stats, left);
    }
    /// col1 = col2
    else if (!isConstColumn(left) && !isConstColumn(right))
    {
        node_stats.selectivity = 0.1; /// TODO add to settings

        for (auto & [child_input_node, stats] : context[left].input_node_stats)
            node_stats.set(child_input_node, stats->clone());

        for (auto & [child_input_node, stats] : context[right].input_node_stats)
            node_stats.set(child_input_node, stats->clone());
    }
    /// 1 = 2
    else
    {
        /// Should never reach here
    }

    return node_stats;
}

ActionNodeStatistics PredicateNodeVisitor::calculateUnaryPredicateFunction(const ActionsDAG::Node * /*node*/, ContextType & /*context*/)
{
    /// TODO implement
    ActionNodeStatistics node_stats;
    return node_stats;
}

ActionNodeStatistics PredicateNodeVisitor::visitEqual(const ActionsDAG::Node * node, ContextType & context)
{
    return calculateBinaryPredicateFunction(node, ColumnStatistics::EQUAL, context);
}

ActionNodeStatistics PredicateNodeVisitor::visitNotEqual(const ActionsDAG::Node * node, ContextType & context)
{
    return calculateBinaryPredicateFunction(node, ColumnStatistics::NOT_EQUAL, context);
}

ActionNodeStatistics PredicateNodeVisitor::visitGreater(const ActionsDAG::Node * node, ContextType & context)
{
    return calculateBinaryPredicateFunction(node, ColumnStatistics::GREATER, context);
}

ActionNodeStatistics PredicateNodeVisitor::visitGreaterOrEqual(const ActionsDAG::Node * node, ContextType & context)
{
    return calculateBinaryPredicateFunction(node, ColumnStatistics::GREATER_OR_EQUAL, context);
}

ActionNodeStatistics PredicateNodeVisitor::visitLess(const ActionsDAG::Node * node, ContextType & context)
{
    return calculateBinaryPredicateFunction(node, ColumnStatistics::LESS, context);
}

ActionNodeStatistics PredicateNodeVisitor::visitLessOrEqual(const ActionsDAG::Node * node, ContextType & context)
{
    return calculateBinaryPredicateFunction(node, ColumnStatistics::LESS_OR_EQUAL, context);
}

ActionNodeStatistics PredicateNodeVisitor::visitAlias(const ActionsDAG::Node * node, ContextType & context)
{
    return visit(node->children.front(), context);
}

ActionNodeStatistics PredicateNodeVisitor::visitArrayJoin(const ActionsDAG::Node * node, ContextType & context)
{
    return ExpressionStatsCalculator::calculateStatistics(node, context);
}

}
