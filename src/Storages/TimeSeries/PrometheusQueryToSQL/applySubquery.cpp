#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySubquery.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void checkExpressionType(const SQLQueryPiece & expression, const ConverterContext & context)
    {
        if (expression.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "Expression {} has type {} and can't be used in a subquery",
                getPromQLText(expression, context),
                expression.type);
        }
    }
}


SQLQueryPiece applySubquery(const PrometheusQueryTree::Subquery * subquery_node, SQLQueryPiece && expression, ConverterContext & context)
{
    checkExpressionType(expression, context);

    expression.node = subquery_node;
    expression.type = ResultType::RANGE_VECTOR;
    /// A range vector has no series order, so an order fixed by an inner sort*() call no longer applies.
    expression.sort_rank_subquery.clear();
    return std::move(expression);
}

}
