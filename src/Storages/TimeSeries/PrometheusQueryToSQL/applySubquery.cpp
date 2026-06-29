#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySubquery.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>


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

    /// A VECTOR_GRID built for an instant vector keeps Prometheus stale markers in its `values` array:
    /// they are filtered out only later, when the grid is finalized as an instant vector (see finalizeInstantVectorAsSQL).
    /// A subquery reuses the same grid as a range vector, so without this step the stale markers would surface
    /// as real matrix samples and would also feed range functions applied over the subquery (see applyFunctionOverRange).
    /// Replace stale-marker grid entries with NULL: `timeSeriesFromGrid` skips NULL entries, which matches
    /// Prometheus dropping the stale step entirely.
    void filterStaleMarkersInVectorGrid(SQLQueryPiece & expression, ConverterContext & context)
    {
        chassert(expression.store_method == StoreMethod::VECTOR_GRID);
        chassert(expression.select_query);

        /// SELECT group,
        ///        arrayMap(x -> if(isNotNull(x) AND reinterpretAsUInt64(assumeNotNull(x)) = <stale_marker>, NULL, x), values) AS values
        /// FROM (<previous select query>)
        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        const String iterator_name = "x";

        /// isNotNull(x) AND reinterpretAsUInt64(assumeNotNull(x)) = 0x7ff0000000000002
        /// (0x7ff0000000000002 is the bit representation of the Prometheus stale marker.)
        ASTPtr is_stale_marker = makeASTFunction(
            "and",
            makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>(iterator_name)),
            makeASTFunction(
                "equals",
                makeASTFunction("reinterpretAsUInt64", makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>(iterator_name))),
                make_intrusive<ASTLiteral>(0x7ff0000000000002ULL)));

        /// if(<is_stale_marker>, NULL, x)
        ASTPtr lambda_body = makeASTFunction(
            "if",
            std::move(is_stale_marker),
            make_intrusive<ASTLiteral>(Field{} /* NULL */),
            make_intrusive<ASTIdentifier>(iterator_name));

        ASTPtr values = makeASTFunction(
            "arrayMap",
            makeASTFunction("lambda", makeASTFunction("tuple", make_intrusive<ASTIdentifier>(iterator_name)), std::move(lambda_body)),
            make_intrusive<ASTIdentifier>(ColumnNames::Values));
        values->setAlias(ColumnNames::Values);
        builder.select_list.push_back(std::move(values));

        context.subqueries.emplace_back(context.subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE);
        builder.from_table = context.subqueries.back().name;

        expression.select_query = builder.getSelectQuery();
    }
}


SQLQueryPiece applySubquery(const PQT::Subquery * subquery_node, SQLQueryPiece && expression, ConverterContext & context)
{
    checkExpressionType(expression, context);

    /// Only VECTOR_GRID carries raw per-series sample values that may contain Prometheus stale markers.
    if (expression.store_method == StoreMethod::VECTOR_GRID)
        filterStaleMarkersInVectorGrid(expression, context);

    expression.node = subquery_node;
    expression.type = ResultType::RANGE_VECTOR;
    return std::move(expression);
}

}
