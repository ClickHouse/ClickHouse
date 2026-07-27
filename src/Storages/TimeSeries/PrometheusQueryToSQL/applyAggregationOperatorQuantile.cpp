#include <Storages/TimeSeries/PrometheusQueryToSQL/applyAggregationOperatorQuantile.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForAggregationOperator.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for the `quantile` aggregation operator.
    void checkArgumentTypes(
        const PQT::AggregationOperator * operator_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context)
    {
        const auto & operator_name = operator_node->operator_name;

        if (arguments.size() != 2)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects 2 arguments, but was called with {} arguments",
                            operator_name, arguments.size());
        }

        const auto & phi_arg = arguments[0];

        if (phi_arg.type != ResultType::SCALAR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects first argument of type {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR,
                            getPromQLText(phi_arg, context), phi_arg.type);
        }

        const auto & vector_arg = arguments[1];

        if (vector_arg.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects second argument of type {}, but expression {} has type {}",
                            operator_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(vector_arg, context), vector_arg.type);
        }
    }

    /// Converts the quantile parameter phi to an AST expression usable in SQL.
    ASTPtr getPhi(SQLQueryPiece && phi_arg, ConverterContext & context)
    {
        switch (phi_arg.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                return timeSeriesScalarToAST(phi_arg.scalar_value, context.scalar_data_type);
            }
            case StoreMethod::SINGLE_SCALAR:
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(phi_arg.select_query), SQLSubqueryType::SCALAR});
                auto subquery_id = make_intrusive<ASTIdentifier>(context.subqueries.back().name);
                /// Wrap with assumeNotNull() because scalar subqueries make their result nullable,
                /// but StoreMethod::SINGLE_SCALAR always means one row.
                return makeASTFunction("assumeNotNull", std::move(subquery_id));
            }
            case StoreMethod::SCALAR_GRID:
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                                "Aggregation operator 'quantile' with a non-constant scalar parameter is not supported");
            }
            default:
            {
                throwUnexpectedStoreMethod(phi_arg, context);
            }
        }
    }
}


SQLQueryPiece applyAggregationOperatorQuantile(
    const PQT::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkArgumentTypes(operator_node, arguments, context);

    auto & phi_arg = arguments[0];
    auto & vector_arg = arguments[1];

    /// If either argument is empty then the result is also empty.
    if (phi_arg.store_method == StoreMethod::EMPTY || vector_arg.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};

    vector_arg = toVectorGrid(std::move(vector_arg), context);
    ASTPtr phi = getPhi(std::move(phi_arg), context);

    auto res = vector_arg;
    res.node = operator_node;

    /// Step 1: aggregate over series, using `new_group` as an intermediate alias to avoid
    /// ambiguity with the input `group` column when the alias and the source column share the same name.
    ASTPtr aggregation_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(vector_arg.select_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        ASTPtr new_group = transformGroupASTForAggregationOperator(
            operator_node, make_intrusive<ASTIdentifier>(ColumnNames::Group), /*drop_metric_name=*/true, res.metric_name_dropped);

        builder.select_list.push_back(std::move(new_group));
        builder.select_list.back()->setAlias(ColumnNames::NewGroup);

        /// quantileExactInclusiveForEach(phi)(values)
        builder.select_list.push_back(addParametersToAggregateFunction(
            makeASTFunction("quantileExactInclusiveForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values)),
            std::move(phi)));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        if (operator_node->by || operator_node->without)
            builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        /// Drop empty-values rows.
        /// If the input has no rows then quantileExactInclusiveForEach(...)([]) returns [], but the number of values
        /// in array must always match the number of steps in SQLQueryPiece (see StoreMethod::VECTOR_GRID),
        /// so we just drop such rows.
        builder.having = makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::Values));

        aggregation_query = builder.getSelectQuery();
    }

    /// Step 2: rename `new_group` back to `group`.
    {
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(aggregation_query), SQLSubqueryType::TABLE});

        SelectQueryBuilder builder;
        builder.from_table = context.subqueries.back().name;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::Group);
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

        res.select_query = builder.getSelectQuery();
    }

    return res;
}

}
