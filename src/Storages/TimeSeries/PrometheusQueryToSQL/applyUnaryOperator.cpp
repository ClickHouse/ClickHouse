#include <Storages/TimeSeries/PrometheusQueryToSQL/applyUnaryOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void checkArgumentTypes(
        const PQT::UnaryOperator * operator_node,
        const SQLQueryPiece & argument,
        const ConverterContext & context)
    {
        const auto & operator_name = operator_node->operator_name;
        if (!(operator_name == "+" || operator_name == "-"))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY, "Unknown unary operator with name {}", operator_name);
        }

        if (!(argument.type == ResultType::SCALAR || argument.type == ResultType::INSTANT_VECTOR))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Operator '{}' expects an argument of type {} or {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR, ResultType::INSTANT_VECTOR,
                            getPromQLQuery(argument, context), argument.type);
        }
    }
}


SQLQueryPiece applyUnaryOperator(
    const PQT::UnaryOperator * operator_node, SQLQueryPiece && argument, ConverterContext & context)
{
    checkArgumentTypes(operator_node, argument, context);
    const auto & operator_name = operator_node->operator_name;

    auto res = argument;
    res.node = operator_node;

    if (operator_name == "+")
        return res;

    /// We support only '+' and '-' as unary operators.
    chassert(operator_name == "-");

    switch (argument.store_method)
    {
        case StoreMethod::EMPTY:
        {
            return res;
        }

        case StoreMethod::CONST_SCALAR:
        {
            res.scalar_value = -argument.scalar_value;
            return res;
        }

        case StoreMethod::SCALAR_GRID:
        case StoreMethod::VECTOR_GRID:
        {
            /// For scalar grid:
            /// SELECT arrayMap(x -> -x, values) AS values
            /// FROM <scalar_grid>
            ///
            /// For vector grid:
            /// SELECT group, arrayMap(x -> -x, values) AS values
            /// FROM <vector_grid>
            SelectQueryParams params;
            if (argument.store_method == StoreMethod::VECTOR_GRID)
                params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

            params.select_list.push_back(makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x")),
                    makeASTFunction("negate", std::make_shared<ASTIdentifier>("x"))),
                std::make_shared<ASTIdentifier>(ColumnNames::Values)));

            params.select_list.back()->setAlias(ColumnNames::Values);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
            params.from_table = context.subqueries.back().name;

            res.select_query = buildSelectQuery(std::move(params));

            return dropMetricName(std::move(res), context);
        }

        case StoreMethod::CONST_STRING:
        case StoreMethod::RAW_DATA:
        {
            /// Can't get in here, the store method CONST_STRING is incompatible
            /// with the allowed argument types (see checkArgumentTypes()).
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Argument for operator '{}' in expression {} has unexpected type {} (store_method: {})",
                            operator_name, getPromQLQuery(argument, context), argument.type, argument.store_method);
        }
    }

    UNREACHABLE();
}

}
