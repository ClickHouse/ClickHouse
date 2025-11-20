#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/finalizeSQL.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/makeSelector.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/modifyEvaluationTime.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/modifyResultTypeAfterSubquery.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    SQLQueryPiece visitNode(const PrometheusQueryTree::Node * node, ConverterContext & context)
    {
        switch (node->node_type)
        {
            case PrometheusQueryTree::NodeType::InstantSelector:
            {
                const auto * instant_selector = static_cast<const PrometheusQueryTree::InstantSelector *>(node);
                return makeSelector(instant_selector, context);
            }

            case PrometheusQueryTree::NodeType::RangeSelector:
            {
                const auto * range_selector = static_cast<const PrometheusQueryTree::RangeSelector *>(node);
                return makeSelector(range_selector, context);
            }

            case PrometheusQueryTree::NodeType::Subquery:
            {
                const auto * subquery_node = static_cast<const PrometheusQueryTree::Subquery *>(node);
                SQLQueryPiece expression = visitNode(subquery_node->getExpression(), context);
                return modifyResultTypeAfterSubquery(subquery_node, std::move(expression), context);
            }

            case PrometheusQueryTree::NodeType::At:
            {
                const auto * at_node = static_cast<const PrometheusQueryTree::At *>(node);
                SQLQueryPiece expression = visitNode(at_node->getExpression(), context);
                return modifyEvaluationTime(at_node, std::move(expression), context);
            }

            case PrometheusQueryTree::NodeType::Function:
            {
                const auto * function = static_cast<const PrometheusQueryTree::Function *>(node);
                std::vector<SQLQueryPiece> arguments;
                for (const auto * arg_node : function->getArguments())
                {
                    arguments.push_back(visitNode(arg_node, context));
                }

                if (isFunctionOverRange(function->function_name))
                    return applyFunctionOverRange(function->function_name, std::move(arguments[0]), node, context);
                else
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", function->function_name);
            }

            default:
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Prometheus query node type {} is not implemented", node->node_type);
            }
        }
    }
}


Converter::Converter(PrometheusQueryTree promql_tree_, PrometheusQueryEvaluationSettings settings_)
    : promql_tree(std::move(promql_tree_))
    , settings(std::move(settings_))
    , result_type(DB::PrometheusQueryToSQL::getResultType(promql_tree, settings))
{
}


ColumnsDescription Converter::getResultColumns() const
{
    return DB::PrometheusQueryToSQL::getResultColumns(promql_tree, settings);
}


ASTPtr Converter::getSQL() const
{
    ConverterContext context{promql_tree, settings};
    auto query_piece = visitNode(promql_tree.getRoot(), context);
    if (settings.evaluation_range)
        query_piece.type = ResultType::RANGE_VECTOR;
    return finalizeSQL(std::move(query_piece), context);
}

}
