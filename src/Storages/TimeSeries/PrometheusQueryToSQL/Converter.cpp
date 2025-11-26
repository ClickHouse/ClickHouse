#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyUnaryOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/finalizeSQL.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultColumns.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/makeSelector.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/modifyEvaluationTime.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/modifyResultTypeAfterSubquery.h>


namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    SQLQueryPiece visitNode(const Node * node, ConverterContext & context)
    {
        switch (node->node_type)
        {
            case NodeType::ScalarLiteral:
            {
                const auto * scalar_node = static_cast<const PQT::ScalarLiteral *>(node);
                return fromLiteral(scalar_node, context);
            }

            case NodeType::StringLiteral:
            {
                const auto * string_node = static_cast<const PQT::StringLiteral *>(node);
                return fromLiteral(string_node, context);
            }

            case NodeType::Duration:
            {
                const auto * duration_node = static_cast<const PQT::Duration *>(node);
                return fromLiteral(duration_node, context);
            }

            case NodeType::InstantSelector:
            {
                const auto * instant_selector = static_cast<const PQT::InstantSelector *>(node);
                return makeSelector(instant_selector, context);
            }

            case NodeType::RangeSelector:
            {
                const auto * range_selector = static_cast<const PQT::RangeSelector *>(node);
                return makeSelector(range_selector, context);
            }

            case NodeType::Subquery:
            {
                const auto * subquery_node = static_cast<const PQT::Subquery *>(node);
                SQLQueryPiece expression = visitNode(subquery_node->getExpression(), context);
                return modifyResultTypeAfterSubquery(subquery_node, std::move(expression), context);
            }

            case NodeType::At:
            {
                const auto * at_node = static_cast<const PQT::At *>(node);
                SQLQueryPiece expression = visitNode(at_node->getExpression(), context);
                return modifyEvaluationTime(at_node, std::move(expression), context);
            }

            case NodeType::Function:
            {
                const auto * function = static_cast<const PQT::Function *>(node);
                std::vector<SQLQueryPiece> arguments;
                for (const auto * arg_node : function->getArguments())
                {
                    arguments.push_back(visitNode(arg_node, context));
                }

                if (isFunctionOverRange(function->function_name))
                    return applyFunctionOverRange(node, function->function_name, std::move(arguments), context);
                else
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", function->function_name);
            }

            case NodeType::UnaryOperator:
            {
                const auto * unary_operator = static_cast<const PQT::UnaryOperator *>(node);
                SQLQueryPiece argument = visitNode(unary_operator->getArgument(), context);
                return applyUnaryOperator(unary_operator, std::move(argument), context);
            }

            default:
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Prometheus query node type {} is not implemented", node->node_type);
            }
        }
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
    if (settings.evaluation_range)
        query_piece.type = ResultType::RANGE_VECTOR;
    return finalizeSQL(std::move(query_piece), context);
}

}
