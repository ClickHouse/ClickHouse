#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>

#include <DataTypes/DataTypesNumber.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyAggregationOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFusedAggregationBinaryOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOffset.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySubquery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyUnaryOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/finalizeSQL.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultColumns.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>
#include <base/scope_guard.h>

#include <utility>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    SQLQueryPiece visitNode(const Node * node, ConverterContext & context);

    /// Converts a subtree keeping its scalar values in `Float64` instead of `context.scalar_data_type`,
    /// see isFunctionOverRangeFloat64ScalarArgument().
    SQLQueryPiece visitNodeWithFloat64Scalars(const Node * node, ConverterContext & context)
    {
        auto saved_scalar_data_type = std::exchange(context.scalar_data_type, std::make_shared<DataTypeFloat64>());
        SCOPE_EXIT({ context.scalar_data_type = std::move(saved_scalar_data_type); });
        return visitNode(node, context);
    }

    SQLQueryPiece visitNode(const Node * node, ConverterContext & context)
    {
        switch (node->node_type)
        {
            case NodeType::Scalar:
            {
                const auto * scalar_node = static_cast<const PrometheusQueryTree::Scalar *>(node);
                return fromLiteral(scalar_node, context);
            }

            case NodeType::StringLiteral:
            {
                const auto * string_node = static_cast<const PrometheusQueryTree::StringLiteral *>(node);
                return fromLiteral(string_node, context);
            }

            case NodeType::InstantSelector:
            {
                const auto * instant_selector = static_cast<const PrometheusQueryTree::InstantSelector *>(node);
                return fromSelector(instant_selector, context);
            }

            case NodeType::RangeSelector:
            {
                const auto * range_selector = static_cast<const PrometheusQueryTree::RangeSelector *>(node);
                return fromSelector(range_selector, context);
            }

            case NodeType::Subquery:
            {
                const auto * subquery_node = static_cast<const PrometheusQueryTree::Subquery *>(node);
                SQLQueryPiece expression = visitNode(subquery_node->getExpression(), context);
                return applySubquery(subquery_node, std::move(expression), context);
            }

            case NodeType::Offset:
            {
                const auto * offset_node = static_cast<const PrometheusQueryTree::Offset *>(node);
                SQLQueryPiece expression = visitNode(offset_node->getExpression(), context);
                return applyOffset(offset_node, std::move(expression), context);
            }

            case NodeType::Function:
            {
                const auto * function = static_cast<const PrometheusQueryTree::Function *>(node);
                const auto & argument_nodes = function->getArguments();
                const bool is_function_over_range = isFunctionOverRange(function->function_name);
                std::vector<SQLQueryPiece> arguments;
                arguments.reserve(argument_nodes.size());
                for (size_t i = 0; i != argument_nodes.size(); ++i)
                {
                    if (is_function_over_range && isFunctionOverRangeFloat64ScalarArgument(function->function_name, i))
                        arguments.push_back(visitNodeWithFloat64Scalars(argument_nodes[i], context));
                    else
                        arguments.push_back(visitNode(argument_nodes[i], context));
                }
                return applyFunction(function, std::move(arguments), context);
            }

            case NodeType::UnaryOperator:
            {
                const auto * unary_operator = static_cast<const PrometheusQueryTree::UnaryOperator *>(node);
                SQLQueryPiece argument = visitNode(unary_operator->getArgument(), context);
                return applyUnaryOperator(unary_operator, std::move(argument), context);
            }

            case NodeType::BinaryOperator:
            {
                const auto * binary_operator = static_cast<const PrometheusQueryTree::BinaryOperator *>(node);

                if (canFuseAggregationBinaryOperator(binary_operator, context))
                {
                    const auto * aggregation
                        = static_cast<const PrometheusQueryTree::AggregationOperator *>(binary_operator->getLeftArgument());
                    SQLQueryPiece argument = visitNode(aggregation->getArguments().at(0), context);
                    return applyFusedAggregationBinaryOperator(binary_operator, std::move(argument), context);
                }

                SQLQueryPiece left_argument = visitNode(binary_operator->getLeftArgument(), context);
                SQLQueryPiece right_argument = visitNode(binary_operator->getRightArgument(), context);
                return applyBinaryOperator(binary_operator, std::move(left_argument), std::move(right_argument), context);
            }

            case NodeType::AggregationOperator:
            {
                const auto * aggregation_operator = static_cast<const PrometheusQueryTree::AggregationOperator *>(node);
                std::vector<SQLQueryPiece> arguments;
                for (const auto * arg_node : aggregation_operator->getArguments())
                {
                    arguments.push_back(visitNode(arg_node, context));
                }
                return applyAggregationOperator(aggregation_operator, std::move(arguments), context);
            }
        }

        UNREACHABLE();
    }
}


Converter::Converter(std::shared_ptr<const PrometheusQueryTree> promql_tree_, PrometheusQueryEvaluationSettings settings_)
    : promql_tree(std::move(promql_tree_))
    , settings(std::move(settings_))
    , result_type(DB::PrometheusQueryToSQL::getResultType(*promql_tree, settings))
{
}


ColumnsDescription Converter::getResultColumns() const
{
    return DB::PrometheusQueryToSQL::getResultColumns(*promql_tree, settings);
}


ASTPtr Converter::getSQL() const
{
    ConverterContext context{promql_tree, settings};
    auto query_piece = visitNode(promql_tree->getRoot(), context);
    query_piece.type = result_type;
    return finalizeSQL(std::move(query_piece), context);
}

}
