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


SQLQueryPiece applySubquery(const PQT::Subquery * subquery_node, SQLQueryPiece && expression, ConverterContext & context)
{
    checkExpressionType(expression, context);

    /// Prometheus stale markers are dropped from an instant-vector grid at its source (see fromSelector), so the
    /// grid reused here as a range vector never carries them. We only need to change the result type.
    expression.node = subquery_node;
    expression.type = ResultType::RANGE_VECTOR;
    return std::move(expression);
}

}
