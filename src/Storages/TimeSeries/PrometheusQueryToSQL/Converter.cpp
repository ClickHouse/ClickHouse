#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunction.h>
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
            case NodeType::Scalar:
            {
                const auto * scalar_node = static_cast<const PQT::Scalar *>(node);
                return fromLiteral(scalar_node, context);
            }

            case NodeType::StringLiteral:
            {
                const auto * string_node = static_cast<const PQT::StringLiteral *>(node);
                return fromLiteral(string_node, context);
            }

            case NodeType::InstantSelector:
            {
                const auto * instant_selector = static_cast<const PQT::InstantSelector *>(node);
                return fromSelector(instant_selector, context);
            }

            case NodeType::RangeSelector:
            {
                const auto * range_selector = static_cast<const PQT::RangeSelector *>(node);
                return fromSelector(range_selector, context);
            }

            case NodeType::Subquery:
            {
                const auto * subquery_node = static_cast<const PQT::Subquery *>(node);
                SQLQueryPiece expression = visitNode(subquery_node->getExpression(), context);
                return applySubquery(subquery_node, std::move(expression), context);
            }

            case NodeType::Offset:
            {
                const auto * offset_node = static_cast<const PQT::Offset *>(node);
                SQLQueryPiece expression = visitNode(offset_node->getExpression(), context);
                return applyOffset(offset_node, std::move(expression), context);
            }

            case NodeType::Function:
            {
                const auto * function = static_cast<const PQT::Function *>(node);
                std::vector<SQLQueryPiece> arguments;
                for (const auto * arg_node : function->getArguments())
                {
                    arguments.push_back(visitNode(arg_node, context));
                }
                return applyFunction(function, std::move(arguments), context);
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
    query_piece.type = result_type;
    return finalizeSQL(std::move(query_piece), context);
}

}
