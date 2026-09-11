#include <Storages/TimeSeries/PrometheusQueryToSQL/applyUnaryOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for the unary operator.
    void checkArgumentTypes(const PQT::UnaryOperator * operator_node, const SQLQueryPiece & argument, const ConverterContext & context)
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
                            getPromQLText(argument, context), argument.type);
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

        case StoreMethod::SINGLE_SCALAR:
        {
            /// SELECT -value AS value FROM <subquery>
            SelectQueryBuilder builder;

            builder.select_list.push_back(makeASTFunction("negate", make_intrusive<ASTIdentifier>(ColumnNames::Value)));
            builder.select_list.back()->setAlias(ColumnNames::Value);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;

            res.select_query = builder.getSelectQuery();
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
            SelectQueryBuilder builder;
            if (argument.store_method == StoreMethod::VECTOR_GRID)
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            builder.select_list.push_back(makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                    makeASTFunction("negate", make_intrusive<ASTIdentifier>("x"))),
                make_intrusive<ASTIdentifier>(ColumnNames::Values)));

            builder.select_list.back()->setAlias(ColumnNames::Values);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;

            res.select_query = builder.getSelectQuery();

            return dropMetricName(std::move(res), context);
        }

        case StoreMethod::CONST_STRING:
        case StoreMethod::RAW_DATA:
        {
            /// Can't get in here because these store methods are incompatible with the allowed argument types
            /// (see checkArgumentTypes()).
            throwUnexpectedStoreMethod(argument, context);
        }
    }

    UNREACHABLE();
}

}
