#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFusedAggregationBinaryOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyMathBinaryOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOneArgumentAggregationOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForAggregationOperator.h>
#include <algorithm>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    using AggregationOperatorNode = PrometheusQueryTree::AggregationOperator;

    const AggregationOperatorNode * tryGetFusableAggregation(const Node * node)
    {
        if (node->node_type != NodeType::AggregationOperator)
            return nullptr;

        const auto * aggregation = static_cast<const AggregationOperatorNode *>(node);

        if (!isOneArgumentAggregationOperator(aggregation->operator_name) || (aggregation->getArguments().size() != 1))
            return nullptr;

        /// `by (__name__)` keeps the metric name; the operator then drops it and must detect duplicate series.
        const auto & labels = aggregation->labels;
        if (aggregation->by && (std::find(labels.begin(), labels.end(), kMetricName) != labels.end()))
            return nullptr;

        return aggregation;
    }

    bool haveSameGrouping(const AggregationOperatorNode * left, const AggregationOperatorNode * right)
    {
        if ((left->by != right->by) || (left->without != right->without))
            return false;

        auto sorted_labels = [](const Strings & labels)
        {
            std::vector<std::string_view> res{labels.begin(), labels.end()};
            std::sort(res.begin(), res.end());
            res.erase(std::unique(res.begin(), res.end()), res.end());
            return res;
        };

        return sorted_labels(left->labels) == sorted_labels(right->labels);
    }
}


bool canFuseAggregationBinaryOperator(const PrometheusQueryTree::BinaryOperator * operator_node, const ConverterContext & context)
{
    if (!isMathBinaryOperator(operator_node->operator_name))
        return false;

    if (operator_node->bool_modifier || operator_node->on || operator_node->ignoring
        || operator_node->group_left || operator_node->group_right)
        return false;

    const auto * left_aggregation = tryGetFusableAggregation(operator_node->getLeftArgument());
    const auto * right_aggregation = tryGetFusableAggregation(operator_node->getRightArgument());

    if (!left_aggregation || !right_aggregation || !haveSameGrouping(left_aggregation, right_aggregation))
        return false;

    const auto * left_argument = left_aggregation->getArguments().at(0);
    const auto * right_argument = right_aggregation->getArguments().at(0);

    if (left_argument->toString(*context.promql_tree) != right_argument->toString(*context.promql_tree))
        return false;

    const auto & left_range = context.node_range_getter.get(left_argument);
    const auto & right_range = context.node_range_getter.get(right_argument);

    return (left_range.start_time == right_range.start_time) && (left_range.end_time == right_range.end_time)
        && (left_range.step == right_range.step) && (left_range.window == right_range.window);
}


SQLQueryPiece applyFusedAggregationBinaryOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node, SQLQueryPiece && argument, ConverterContext & context)
{
    const auto * left_aggregation = static_cast<const AggregationOperatorNode *>(operator_node->getLeftArgument());
    const auto * right_aggregation = static_cast<const AggregationOperatorNode *>(operator_node->getRightArgument());

    auto left_transform = getOneArgumentAggregationTransform(left_aggregation->operator_name);
    auto right_transform = getOneArgumentAggregationTransform(right_aggregation->operator_name);
    chassert(left_transform && right_transform);

    if (argument.type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Aggregation operator '{}' expects an argument of type {}, but expression {} has type {}",
                        left_aggregation->operator_name, ResultType::INSTANT_VECTOR,
                        getPromQLText(argument, context), argument.type);
    }

    if (argument.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};

    argument = toVectorGrid(std::move(argument), context);

    auto res = argument;
    res.node = operator_node;
    res.type = operator_node->result_type;

    /// Step 1:
    /// SELECT <group> AS new_group,
    ///        arrayMap((x, y) -> f(x, y), <left_aggregate>(values), <right_aggregate>(values)) AS values
    /// FROM argument
    /// [GROUP BY new_group]
    ASTPtr aggregation_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        ASTPtr new_group = transformGroupASTForAggregationOperator(
            left_aggregation, make_intrusive<ASTIdentifier>(ColumnNames::Group), /*drop_metric_name=*/true, res.metric_name_dropped);

        builder.select_list.push_back(std::move(new_group));
        builder.select_list.back()->setAlias(ColumnNames::NewGroup);

        builder.select_list.push_back(makeASTFunction(
            "arrayMap",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y")),
                applyMathBinaryOperatorToAST(
                    operator_node->operator_name, make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y"))),
            left_transform(make_intrusive<ASTIdentifier>(ColumnNames::Values), context.scalar_data_type),
            right_transform(make_intrusive<ASTIdentifier>(ColumnNames::Values), context.scalar_data_type)));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        if (left_aggregation->by || left_aggregation->without)
            builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        aggregation_query = builder.getSelectQuery();
    }

    /// Step 2: rename `new_group` back to `group` and drop empty grids (a WHERE, so `values` cannot bind to the input column
    /// under prefer_column_name_to_alias).
    {
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(aggregation_query), SQLSubqueryType::TABLE});

        SelectQueryBuilder builder;
        builder.from_table = context.subqueries.back().name;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::Group);
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        builder.where = makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::Values));

        res.select_query = builder.getSelectQuery();
    }

    return res;
}

}
