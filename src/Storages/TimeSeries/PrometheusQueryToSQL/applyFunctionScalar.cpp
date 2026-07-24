#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionScalar.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void checkArgumentTypes(const PQT::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;
        if (arguments.size() != 1)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects {} arguments, but was called with {} arguments",
                            function_name, 1, arguments.size());
        }

        const auto & argument = arguments[0];
        if (argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects an argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(argument, context), argument.type);
        }
    }
}


SQLQueryPiece applyFunctionScalar(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    chassert(isFunctionScalar(function_name));

    checkArgumentTypes(function_node, arguments, context);
    auto & argument = arguments[0];

    auto res = argument;
    res.node = function_node;
    res.type = ResultType::SCALAR;

    switch (argument.store_method)
    {
        case StoreMethod::EMPTY:
        case StoreMethod::CONST_SCALAR:
        case StoreMethod::SINGLE_SCALAR:
        case StoreMethod::SCALAR_GRID:
        {
            /// These store methods are already compatible with scalars, so we do nothing here.
            return res;
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// If the argument contains time series we have to do some aggregation.

            /// SELECT arrayMap(x, y -> if(x = 1, assumeNotNull(y), NaN), countForEach(values), anyForEach(values)) AS values
            /// FROM <vector_grid>
            SelectQueryBuilder builder;

            builder.select_list.push_back(makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y")),
                    makeASTFunction(
                        "if",
                        makeASTFunction("equals", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(1)),
                        makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>("y")),
                        timeSeriesScalarToAST(std::numeric_limits<Float64>::quiet_NaN(), context.scalar_data_type))),
                makeASTFunction("countForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values)),
                makeASTFunction("anyForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values))));

            builder.select_list.back()->setAlias(ColumnNames::Values);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;

            res.select_query = builder.getSelectQuery();
            res.store_method = StoreMethod::SCALAR_GRID;

            return res;
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
