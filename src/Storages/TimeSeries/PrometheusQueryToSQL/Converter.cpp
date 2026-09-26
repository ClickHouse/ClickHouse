#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyAggregationOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFusedAggregationBinaryOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOffset.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySubquery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyUnaryOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/finalizeSQL.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultColumns.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void rejectReservedLabelName(std::string_view label_name)
    {
        if (label_name == kDroppedMetricNameMarker)
        {
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "Label name {} is reserved",
                quoteString(label_name));
        }
    }

    void rejectReservedLabelNames(const Node * node)
    {
        switch (node->node_type)
        {
            case NodeType::InstantSelector:
            {
                const auto * selector = static_cast<const PrometheusQueryTree::InstantSelector *>(node);
                for (const auto & matcher : selector->matchers)
                    rejectReservedLabelName(matcher.label_name);
                break;
            }

            case NodeType::AggregationOperator:
            {
                const auto * aggregation_operator = static_cast<const PrometheusQueryTree::AggregationOperator *>(node);
                for (const auto & label : aggregation_operator->labels)
                    rejectReservedLabelName(label);

                if (aggregation_operator->operator_name == "count_values")
                {
                    const auto & arguments = aggregation_operator->getArguments();
                    if (!arguments.empty() && arguments[0]->node_type == NodeType::StringLiteral)
                        rejectReservedLabelName(static_cast<const PrometheusQueryTree::StringLiteral *>(arguments[0])->string);
                }
                break;
            }

            case NodeType::BinaryOperator:
            {
                const auto * binary_operator = static_cast<const PrometheusQueryTree::BinaryOperator *>(node);
                for (const auto & label : binary_operator->labels)
                    rejectReservedLabelName(label);
                for (const auto & label : binary_operator->extra_labels)
                    rejectReservedLabelName(label);
                break;
            }

            case NodeType::Function:
            {
                const auto * function = static_cast<const PrometheusQueryTree::Function *>(node);
                if (function->function_name == "label_replace" || function->function_name == "label_join")
                {
                    const auto & arguments = function->getArguments();
                    if (arguments.size() > 1 && arguments[1]->node_type == NodeType::StringLiteral)
                        rejectReservedLabelName(static_cast<const PrometheusQueryTree::StringLiteral *>(arguments[1])->string);

                    size_t source_end = arguments.size();
                    if (function->function_name == "label_replace" && source_end > 4)
                        source_end = 4;
                    for (size_t i = 3; i < source_end; ++i)
                        if (arguments[i]->node_type == NodeType::StringLiteral)
                            rejectReservedLabelName(static_cast<const PrometheusQueryTree::StringLiteral *>(arguments[i])->string);
                }
                break;
            }

            default:
                break;
        }

        for (const auto * child : node->children)
            rejectReservedLabelNames(child);
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
                std::vector<SQLQueryPiece> arguments;
                for (const auto * arg_node : function->getArguments())
                {
                    arguments.push_back(visitNode(arg_node, context));
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
    rejectReservedLabelNames(promql_tree->getRoot());
    auto query_piece = visitNode(promql_tree->getRoot(), context);
    query_piece.type = result_type;
    return finalizeSQL(std::move(query_piece), context);
}

}
